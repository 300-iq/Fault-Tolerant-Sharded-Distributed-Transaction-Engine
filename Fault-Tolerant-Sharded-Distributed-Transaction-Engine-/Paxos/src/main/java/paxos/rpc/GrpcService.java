package paxos.rpc;

import io.grpc.stub.StreamObserver;
import paxos.common.MessageAudit;
import paxos.common.Types;
import paxos.common.Types.*;
import paxos.node.*;
import paxos.proto.*;
import paxos.proto.PaxosProto;
import paxos.proto.PaxosProto.*;
import paxos.utils.PrintHelper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import java.util.concurrent.TimeUnit;

/* I took aid of chat Gpt for  RPC implementations like checkpointing as I was unfamiliar with digests etc and I was also facing some

    errors during catchup and new view for which to debug I took help of chatGPT.

 */

public class GrpcService extends NodeServiceGrpc.NodeServiceImplBase {

    private final NodeState state;
    private final Replication replication;
    private final StateMachine stateMachine;
    private final Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs;
    private final Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> twoPcStubs;
    private final ElectionCoordinator electionCoordinator;
    private final String logPrefix;
    private static final boolean PERFORMANCE_MODE = Boolean.parseBoolean(System.getProperty("performanceMode", "false"));
    private static final long FOLLOWER_FORWARD_TIMEOUT_MS = Long.getLong("followerForwardTimeoutMs", 500L);
    private static final long TWO_PC_RPC_TIMEOUT_MS = Long.getLong("twoPcRpcTimeoutMs", 1200L);


    private static final long INTRA_SHARD_LOCK_WAIT_MS =
            Long.getLong("intraShardLockWaitMs", PERFORMANCE_MODE ? 80L : 100L);
    private static final long COORD_SENDER_LOCK_WAIT_MS =
            Long.getLong("twoPcSenderLockWaitMs", PERFORMANCE_MODE ? 80L : 100L);
    private static final long TWO_PC_SWEEP_INTERVAL_MS =
            Long.getLong("twoPcSweepIntervalMs", 200L);
    private static final long COORD_PREPARE_TIMEOUT_MS =
            Long.getLong("twoPcCoordPrepareTimeoutMs", PERFORMANCE_MODE ? 1500L : 2500L);
    private static final long COORD_DECISION_TIMEOUT_MS =
            Long.getLong("twoPcCoordDecisionTimeoutMs", PERFORMANCE_MODE ? 2000L : 3000L);
    private static final int CLIENT_MAX_ATTEMPTS = Integer.getInteger(
            "clientMaxAttempts", PERFORMANCE_MODE ? 10 : 20);
    private final boolean checkpointingEnabled;
    private final int checkpointPeriod;
    private final java.util.concurrent.ScheduledExecutorService twoPcTimer;
    public GrpcService(NodeState state,
                       Replication replication,
                       StateMachine stateMachine,
                       Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs,
                       Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> twoPcStubs,
                       ElectionCoordinator electionCoordinator) {
        this.state = state;
        this.replication = replication;
        this.stateMachine = stateMachine;
        this.stubs = stubs;
        this.twoPcStubs = twoPcStubs;
        this.electionCoordinator = electionCoordinator;
        this.logPrefix = "[Node " + state.getNodeConfig().getNodeId() + "] ";
        this.checkpointingEnabled = state.isCheckpointingEnabled();
        this.checkpointPeriod = state.getCheckpointPeriod();

        this.twoPcTimer = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "2pc-timers-" + state.getNodeConfig().getNodeId());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thr, ex) -> {
                try {
                    System.err.println("2PC timer thread died on node " + state.getNodeConfig().getNodeId() + ": " + ex);
                } catch (Throwable ignored) { }
            });
            return t;
        });
        this.twoPcTimer.scheduleAtFixedRate(
                this::scanTwoPcTimeouts,
                TWO_PC_SWEEP_INTERVAL_MS,
                TWO_PC_SWEEP_INTERVAL_MS,
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        try {
            twoPcTimer.shutdownNow();
        } catch (Exception ignored) { }
    }

    @Override
    public void getAudit(Empty e, StreamObserver<PaxosProto.AuditDump> resp) {
        java.util.List<PaxosProto.AuditEntryDump> list = new java.util.ArrayList<>();
        for (MessageAudit.Entry en : state.getAudit().snapshot()) {
            PaxosProto.AuditEntryDump.Builder b = PaxosProto.AuditEntryDump.newBuilder()
                    .setTs(en.ts)
                    .setSelf(en.selfNodeId)
                    .setDir(en.dir.name())
                    .setKind(en.kind.name())
                    .setPeer(en.peerNodeId)
                    .setBallotRound(en.ballotRound)
                    .setBallotNode(en.ballotNode)
                    .setSeq(en.seq)
                    .setReqTs(en.reqTs)
                    .setAmount(en.amount)
                    .setNote(en.note == null ? "" : en.note);
            if (en.clientId != null) b.setClientId(en.clientId);
            if (en.sender != null) b.setSender(en.sender);
            if (en.receiver != null) b.setReceiver(en.receiver);
            list.add(b.build());
        }
        resp.onNext(PaxosProto.AuditDump.newBuilder().addAllEntries(list).build());
        resp.onCompleted();
    }

    @Override
    public void submit(ClientRequest req, StreamObserver<Reply> resp) {

        if (state.isLfSuspended() && state.getIsLeader()) {
            Ballot ballot = state.getCurrentBallot() != null ? state.getCurrentBallot().toProto() : Ballot.getDefaultInstance();
            Reply r = Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Leader suspended for current set")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(ballot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
            resp.onNext(r);
            resp.onCompleted();
            return;
        }
        Reply r = handleClientSubmit(req);
        resp.onNext(r);
        resp.onCompleted();
    }

    @Override
    public void accept(AcceptMsg msg, StreamObserver<Accepted> resp) {
        if (state.isLfSuspended()) {
            Accepted rejected = Accepted.newBuilder()
                    .setB(msg.getBallot())
                    .setSeq(msg.getSeq())
                    .setFromNode(state.getNodeConfig().getNodeId())
                    .setOk(false)
                    .setReason("Node suspended for current set")
                    .build();
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }
        BallotNum ballot = Types.BallotNum.of(msg.getBallot());
        int self = state.getNodeConfig().getNodeId();

        log("ACCEPT recv seq=%d ballot=%s from leader=%d", msg.getSeq(), ballotString(ballot), msg.getBallot().getNodeId());

        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.ACCEPT,
                msg.getBallot().getNodeId(), msg.getBallot(), msg.getSeq(), msg.getReq(), null);

        if (!state.isSelfAllowed()) {
            Accepted rejected = Accepted.newBuilder()
                    .setB(msg.getBallot())
                    .setSeq(msg.getSeq())
                    .setFromNode(self)
                    .setOk(false)
                    .setReason("Node not live")
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                    msg.getBallot().getNodeId(), rejected.getB(), rejected.getSeq(), null, "nack: Node not live");
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }
        int leaderIdAccept = msg.getBallot().getNodeId();
        if (leaderIdAccept != 0 && !state.isPeerAllowed(leaderIdAccept)) {
            Accepted rejected = Accepted.newBuilder()
                    .setB(msg.getBallot())
                    .setSeq(msg.getSeq())
                    .setFromNode(self)
                    .setOk(false)
                    .setReason("Leader not in live set")
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                    msg.getBallot().getNodeId(), rejected.getB(), rejected.getSeq(), null, "nack: Leader not in live set");
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }


        if (msg.getSeq() <= 0 || msg.getSeq() > 1000000) {
            log("ACCEPT reject seq=%d invalid sequence number", msg.getSeq());
            Accepted rejected = Accepted.newBuilder()
                    .setB(msg.getBallot())
                    .setSeq(msg.getSeq())
                    .setFromNode(self)
                    .setOk(false)
                    .setReason("Invalid sequence number")
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                    msg.getBallot().getNodeId(), rejected.getB(), rejected.getSeq(), null, "nack: Invalid sequence number");
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }


        int cpAccept = state.getLastCheckpointSeq();
        if (msg.getSeq() <= cpAccept) {
            log("ACCEPT reject seq=%d at/below checkpoint=%d", msg.getSeq(), cpAccept);
            Accepted rejected = Accepted.newBuilder()
                    .setB(msg.getBallot())
                    .setSeq(msg.getSeq())
                    .setFromNode(self)
                    .setOk(false)
                    .setReason("Below checkpoint")
                    .build();
            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                    msg.getBallot().getNodeId(), rejected.getB(), rejected.getSeq(), null, "nack: Below checkpoint");
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }

        if (!state.acceptBallot(ballot)) {
            BallotNum hint = state.getHighestSeenBallot();
            log("ACCEPT reject seq=%d stale ballot=%s hint=%s", msg.getSeq(), ballotString(ballot), ballotString(hint));
            Ballot replyBallot = hint != null ? hint.toProto() : (state.getCurrentBallot() != null ? state.getCurrentBallot().toProto() : msg.getBallot());
            Accepted rejected = Accepted.newBuilder()
                    .setB(replyBallot)
                    .setSeq(msg.getSeq())
                    .setFromNode(self)
                    .setOk(false)
                    .setReason("Higher ballot promised")
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                    msg.getBallot().getNodeId(), rejected.getB(), rejected.getSeq(), null, "nack: Higher ballot promised");
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }

        state.learnLeader(ballot, msg.getBallot().getNodeId());
        electionCoordinator.noteBallotSeen(ballot);

        Types.LogEntry logEntry = state.getLog().computeIfAbsent(msg.getSeq(),
                s -> new Types.LogEntry(s, msg.getReq(), ballot));

        if (logEntry.getPhase().ordinal() >= Phase.ACCEPTED.ordinal()
                && logEntry.getBallot() != null && ballot != null
                && logEntry.getBallot().equals(ballot)
                && !Objects.equals(logEntry.getReq(), msg.getReq())) {
            log("ACCEPT reject seq=%d conflicting payload for same ballot", msg.getSeq());
            Accepted rejected = Accepted.newBuilder()
                    .setB(msg.getBallot())
                    .setSeq(msg.getSeq())
                    .setFromNode(self)
                    .setOk(false)
                    .setReason("Conflicting value for same ballot/seq")
                    .build();
            resp.onNext(rejected);
            resp.onCompleted();
            return;
        }
        if (logEntry.getPhase() != Phase.EXECUTED) {
            logEntry.setReq(msg.getReq());
            logEntry.setBallot(ballot);
            logEntry.setTwoPcTag(msg.getTwoPcTag());
            logEntry.advancePhase(Phase.ACCEPTED);
        }

        else if (!Objects.equals(logEntry.getReq(), msg.getReq())) {
            log("ACCEPT seq=%d ignored conflicting payload (already executed)", msg.getSeq());
        }
        state.advanceNextSeqIfNeeded(msg.getSeq());

        electionCoordinator.gotLeaderMessage();

        log("ACCEPT ok seq=%d ballot=%s", msg.getSeq(), ballotString(ballot));
        Accepted ok = Accepted.newBuilder()
                .setB(msg.getBallot())
                .setSeq(msg.getSeq())
                .setFromNode(self)
                .setOk(true)
                .build();

        MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                msg.getBallot().getNodeId(), ok.getB(), ok.getSeq(), null, "ok");
        resp.onNext(ok);
        resp.onCompleted();


        try {
            stateMachine.executeStateMachine();
        } catch (Throwable ignored) { }
    }

    @Override
    public void commit(CommitMsg msg, StreamObserver<Empty> resp) {
        if (state.isLfSuspended()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        BallotNum ballot = Types.BallotNum.of(msg.getBallot());
        log("COMMIT recv seq=%d ballot=%s", msg.getSeq(), ballotString(ballot));

        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.COMMIT,
                msg.getBallot().getNodeId(), msg.getBallot(), msg.getSeq(), msg.getReq(), null);

        // Note: Removed isSelfAllowed() check here.
        // A node must always accept commits from the leader, even during recovery catchup
        // when the node hasn't yet processed its own setLivePeers message.
        // The "allowed" check should only apply to initiating operations, not receiving updates.

        state.learnLeader(ballot, msg.getBallot().getNodeId());
        electionCoordinator.noteBallotSeen(ballot);


        int cpCommit = state.getLastCheckpointSeq();
        if (msg.getSeq() <= cpCommit) {
            final ClientRequest rr = (msg.getReq() == null ? Types.NOOP : msg.getReq());

            Types.LogEntry le = state.getLog().get(msg.getSeq());
            if (le == null) {
                le = new Types.LogEntry(msg.getSeq(), rr, ballot);
                state.getLog().put(msg.getSeq(), le);
            }
            if (le.getPhase().ordinal() < Phase.EXECUTED.ordinal()) {
                le.setReq(rr);
                le.setBallot(ballot);
                le.setTwoPcTag(msg.getTwoPcTag());
                le.advancePhase(Phase.EXECUTED);
                le.getExecResult().complete(Boolean.TRUE);
            }

            if (!Types.isNoop(rr)) {
                Types.ClientState cs = state.getClientStates().computeIfAbsent(rr.getClientId(), k -> new Types.ClientState());
                Reply dummy = Reply.newBuilder()
                        .setSuccess(true)
                        .setMessage("OK")
                        .setTimestamp(rr.getTimestamp())
                        .setLeaderBallot(msg.getBallot())
                        .setResultType(Reply.ResultType.SUCCESS)
                        .build();
                synchronized (cs) {
                    cs.recordReply(rr.getTimestamp(), dummy);
                }
            }
            log("COMMIT <=cp seq=%d acknowledged for dedup (cp=%d)", msg.getSeq(), cpCommit);
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }

        Types.LogEntry logEntry = state.getLog().computeIfAbsent(msg.getSeq(),
                s -> new Types.LogEntry(s, msg.getReq(), ballot));

        if (logEntry.getPhase() != Phase.EXECUTED) {
            logEntry.setReq(msg.getReq());
            logEntry.setBallot(ballot);
            logEntry.setTwoPcTag(msg.getTwoPcTag());
            logEntry.advancePhase(Phase.COMMITTED);
        } else if (!Objects.equals(logEntry.getReq(), msg.getReq())) {
            log("COMMIT seq=%d arrived with different payload after execution; ignoring", msg.getSeq());
        }
        state.advanceNextSeqIfNeeded(msg.getSeq());

        electionCoordinator.gotLeaderMessage();

        flushStateMachine();
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void getStatus(StatusQuery q, StreamObserver<StatusReply> resp) {
        flushStateMachine();
        int seq = q.getSeq();
        Types.LogEntry logEntry = state.getLog().get(seq);
        TxnStatus st = TxnStatus.X;
        if (logEntry != null) {
            switch (logEntry.getPhase()) {
                case ACCEPTED -> st = TxnStatus.A;
                case COMMITTED -> st = TxnStatus.C;
                case EXECUTED -> st = TxnStatus.E;
                default -> st = TxnStatus.X;
            }
        }

        if (st != TxnStatus.E && seq > 0 && seq <= state.getLastCheckpointSeq()) {
            st = TxnStatus.E;
        }
        resp.onNext(StatusReply.newBuilder()
                .setNodeId(state.getNodeConfig().getNodeId())
                .setSeq(seq)
                .setStatus(st)
                .build());
        resp.onCompleted();
    }

    @Override
    public void getLog(Empty e, StreamObserver<LogDump> resp) {

        flushStateMachine();
        List<LogEntryDump> list = new ArrayList<>();
        state.getLog().values().stream().sorted(Comparator.comparingInt(LogEntry::getSeq)).forEach(logEntry -> {
            ClientRequest r = logEntry.getReq();
            boolean isNoop = Types.isNoop(r);
            String payload = isNoop ? "noop" : (r.getSender() + "->" + r.getReceiver() + ":" + r.getAmount());
            TwoPcTag tag = logEntry.getTwoPcTag();
            if (tag == TwoPcTag.NONE || isNoop) {
                String phase = logEntry.getPhase().name();
                list.add(LogEntryDump.newBuilder().setSeq(logEntry.getSeq()).setPhase(phase)
                        .setClientId(isNoop ? "" : r.getClientId())
                        .setTs(isNoop ? 0 : r.getTimestamp())
                        .setPayload(payload)
                        .setTwoPcTag(tag)
                        .build());
            } else {
                // PREPARE and COMMIT/ABORT now have separate sequence numbers, display each entry as-is
                String phase = logEntry.getPhase().name();
                list.add(LogEntryDump.newBuilder().setSeq(logEntry.getSeq()).setPhase(phase)
                        .setClientId(r.getClientId())
                        .setTs(r.getTimestamp())
                        .setPayload(payload)
                        .setTwoPcTag(tag)
                        .build());
            }
        });
        resp.onNext(LogDump.newBuilder().addAllEntries(list).build());
        resp.onCompleted();
    }

    @Override
    public void getDB(Empty e, StreamObserver<DBDump> resp) {
        flushStateMachine();
        List<KV> kvs = new ArrayList<>();
        state.getDatabase().snapshot().forEach((k, v) -> kvs.add(KV.newBuilder().setKey(k).setValue(v).build()));
        resp.onNext(DBDump.newBuilder().addAllBalances(kvs).build());
        resp.onCompleted();
    }


    @Override
    public void getDBOnDisk(Empty e, StreamObserver<DBDump> resp) {
        flushStateMachine();
        List<KV> kvs = new ArrayList<>();
        Map<String,Integer> snap = state.getDatabase().diskSnapshot();
        for (Map.Entry<String,Integer> ent : snap.entrySet()) {
            kvs.add(KV.newBuilder().setKey(ent.getKey()).setValue(ent.getValue()).build());
        }
        resp.onNext(DBDump.newBuilder().addAllBalances(kvs).build());
        resp.onCompleted();
    }

    @Override
    public void getModifiedDB(Empty e, StreamObserver<DBDump> resp) {
        flushStateMachine();
        List<KV> kvs = new ArrayList<>();
        Map<String,Integer> snap = state.getDatabase().snapshot();
        // Use modified set - only COMMIT adds to it, PREPARE/ABORT don't touch it
        java.util.Set<String> modified = state.getModifiedKeysSnapshot();
        if (modified != null && !modified.isEmpty()) {
            for (String key : modified) {
                Integer v = snap.get(key);
                if (v != null) {
                    kvs.add(KV.newBuilder().setKey(key).setValue(v).build());
                }
            }
        }
        resp.onNext(DBDump.newBuilder().addAllBalances(kvs).build());
        resp.onCompleted();
    }

    @Override
    public void getCheckpointSnapshot(Empty e, StreamObserver<DBDump> resp) {
        List<KV> kvs = new ArrayList<>();
        Map<String,Integer> snap = state.getLastCheckpointSnapshot();
        if (snap == null || snap.isEmpty()) {
            resp.onNext(DBDump.newBuilder().addAllBalances(kvs).build());
            resp.onCompleted();
            return;
        }
        for (Map.Entry<String,Integer> ent : snap.entrySet()) {
            kvs.add(KV.newBuilder().setKey(ent.getKey()).setValue(ent.getValue()).build());
        }
        resp.onNext(DBDump.newBuilder().addAllBalances(kvs).build());
        resp.onCompleted();
    }



    @Override
    public void prepare(PrepareMsg msg, StreamObserver<PromiseMsg> resp) {
        if (state.isLfSuspended()) {
            PromiseMsg out = PromiseMsg.newBuilder()
                    .setBallot(msg.getBallot())
                    .setOk(false)
                    .setReason("Node suspended for current set")
                    .setCheckpointSeq(state.getLastCheckpointSeq())
                    .setCheckpointDigest(state.getLastCheckpointDigest())
                    .setNodeId(state.getNodeConfig().getNodeId())
                    .build();
            resp.onNext(out);
            resp.onCompleted();
            return;
        }

        if (!state.isSelfAllowed()) {
            PromiseMsg out = PromiseMsg.newBuilder()
                    .setBallot(msg.getBallot())
                    .setOk(false)
                    .setReason("Node not live")
                    .setCheckpointSeq(state.getLastCheckpointSeq())
                    .setCheckpointDigest(state.getLastCheckpointDigest())
                    .setNodeId(state.getNodeConfig().getNodeId())
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.PROMISE,
                    msg.getBallot().getNodeId(), out.getBallot(), null, null, "nack: Node not live");
            resp.onNext(out);
            resp.onCompleted();
            return;
        }
        int proposerId = msg.getBallot().getNodeId();
        if (proposerId != 0 && !state.isPeerAllowed(proposerId)) {
            PromiseMsg out = PromiseMsg.newBuilder()
                    .setBallot(msg.getBallot())
                    .setOk(false)
                    .setReason("Proposer not in live set")
                    .setCheckpointSeq(state.getLastCheckpointSeq())
                    .setCheckpointDigest(state.getLastCheckpointDigest())
                    .setNodeId(state.getNodeConfig().getNodeId())
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.PROMISE,
                    proposerId, out.getBallot(), null, null, "nack: Proposer not in live set");
            resp.onNext(out);
            resp.onCompleted();
            return;
        }

        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.PREPARE,
                msg.getBallot().getNodeId(), msg.getBallot(), null, null, null);
        electionCoordinator.gotPrepareMessage();
        BallotNum ballot = BallotNum.of(msg.getBallot());
        log("PREPARE recv ballot=%s from=%d", ballotString(ballot), msg.getBallot().getNodeId());

        electionCoordinator.recordDeferredPrepare(ballot);


        if (!electionCoordinator.shouldAcceptPrepareNow(ballot)) {
            BallotNum pending = electionCoordinator.peekDeferredPrepare();
            BallotNum highest = state.getHighestSeenBallot();
            log("PREPARE defer (timer not expired) ballot=%s pending=%s highest=%s", ballotString(ballot), ballotString(pending), ballotString(highest));
            PromiseMsg out = PromiseMsg.newBuilder()
                    .setBallot(msg.getBallot())
                    .setOk(false)
                    .setReason("Timer not expired")
                    .setCheckpointSeq(state.getLastCheckpointSeq())
                    .setCheckpointDigest(state.getLastCheckpointDigest())
                    .setNodeId(state.getNodeConfig().getNodeId())
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.PROMISE,
                    msg.getBallot().getNodeId(), out.getBallot(), null, null, "nack: Timer not expired");
            resp.onNext(out);
            resp.onCompleted();
            return;
        }

        if (!state.promiseBallot(ballot)) {
            BallotNum hint = state.getHighestSeenBallot();
            log("PREPARE reject ballot=%s promised=%s", ballotString(ballot), ballotString(hint));
            Ballot replyBallot = hint != null ? hint.toProto() : msg.getBallot();
            PromiseMsg out = PromiseMsg.newBuilder()
                    .setBallot(replyBallot)
                    .setOk(false)
                    .setReason("Higher ballot promised")
                    .setCheckpointSeq(state.getLastCheckpointSeq())
                    .setCheckpointDigest(state.getLastCheckpointDigest())
                    .setNodeId(state.getNodeConfig().getNodeId())
                    .build();

            MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.PROMISE,
                    msg.getBallot().getNodeId(), out.getBallot(), null, null, "nack: Higher ballot promised");
            resp.onNext(out);
            resp.onCompleted();
            return;
        }

        electionCoordinator.noteBallotSeen(ballot);
        state.learnLeader(ballot, msg.getBallot().getNodeId());
        electionCoordinator.gotLeaderMessage();
        log("PREPARE ok ballot=%s", ballotString(ballot));

        PromiseMsg.Builder out = PromiseMsg.newBuilder()
                .setBallot(msg.getBallot())
                .setOk(true)
                .setCheckpointSeq(state.getLastCheckpointSeq())
                .setCheckpointDigest(state.getLastCheckpointDigest())
                .setNodeId(state.getNodeConfig().getNodeId());
        state.getLog().values().stream()
                .sorted(Comparator.comparingInt(Types.LogEntry::getSeq))
                .forEach(le -> {
                    if (le.getPhase().ordinal() >= Phase.ACCEPTED.ordinal()) {
                        ClientRequest r = le.getReq();
                        if (r == null) r = Types.NOOP;
                        out.addAcc(AcceptMsg.newBuilder()
                                .setBallot(le.getBallot().toProto())
                                .setSeq(le.getSeq())
                                .setReq(r)
                                .setTwoPcTag(le.getTwoPcTag())
                                .build());
                    }
                });

        MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.PROMISE,
                msg.getBallot().getNodeId(), msg.getBallot(), null, null, "ok");
        resp.onNext(out.build());
        resp.onCompleted();
    }

    @Override
    public void newView(NewViewMsg msg, StreamObserver<Empty> resp) {
        if (state.isLfSuspended()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }

        if (!state.isSelfAllowed()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        int leaderIdNv = msg.getBallot().getNodeId();
        if (leaderIdNv != 0 && !state.isPeerAllowed(leaderIdNv)) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        BallotNum ballot = Types.BallotNum.of(msg.getBallot());

        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.NEW_VIEW,
                msg.getBallot().getNodeId(), msg.getBallot(), null, null, "accepts=" + msg.getAcceptsCount());
        if (msg.getCheckpointSeq() > 0) {
            handleCheckpointMetadata(msg.getCheckpointSeq(), msg.getCheckpointDigest(), leaderIdNv);
        }
        state.learnLeader(ballot, msg.getBallot().getNodeId());
        electionCoordinator.viewHistory().add(msg);
        log("NEW-VIEW apply ballot=%s accepts=%d", ballotString(ballot), msg.getAcceptsCount());
        electionCoordinator.noteBallotSeen(ballot);
        electionCoordinator.gotLeaderMessage();

        Set<Integer> acceptedSeqs = new HashSet<>();
        int maxSeq = 0;
        for (AcceptMsg a : msg.getAcceptsList()) {
            if (a.getSeq() <= msg.getCheckpointSeq()) {
                continue;
            }
            maxSeq = Math.max(maxSeq, a.getSeq());
            acceptedSeqs.add(a.getSeq());
            BallotNum acceptBallot = Types.BallotNum.of(a.getBallot());
            var le = state.getLog().computeIfAbsent(a.getSeq(),
                    s -> new Types.LogEntry(s, a.getReq(), acceptBallot));
            if (le.getPhase() != Phase.EXECUTED) {
                le.setReq(a.getReq());
                le.setBallot(acceptBallot);
                le.setTwoPcTag(a.getTwoPcTag());
                le.advancePhase(Phase.ACCEPTED);
            } else if (!Objects.equals(le.getReq(), a.getReq())) {
                log("NEW-VIEW seq=%d ignored conflicting payload (already executed)", a.getSeq());
            }
        }
        if (maxSeq > 0) {
            state.advanceNextSeqIfNeeded(maxSeq);
        }

        int pruned = state.pruneLogAfterNewView(msg.getCheckpointSeq(), acceptedSeqs);
        if (pruned > 0) {
            log("NEW-VIEW pruned %d unapplied entries beyond checkpoint=%d", pruned, msg.getCheckpointSeq());
        }

        electionCoordinator.gotLeaderMessage();

        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void newViewWithAcks(NewViewMsg msg, StreamObserver<NewViewReply> resp) {
        if (state.isLfSuspended()) {
            resp.onNext(NewViewReply.newBuilder().build());
            resp.onCompleted();
            return;
        }

        if (!state.isSelfAllowed()) {
            resp.onNext(NewViewReply.newBuilder().build());
            resp.onCompleted();
            return;
        }
        int leaderIdNv = msg.getBallot().getNodeId();
        if (leaderIdNv != 0 && !state.isPeerAllowed(leaderIdNv)) {
            resp.onNext(NewViewReply.newBuilder().build());
            resp.onCompleted();
            return;
        }
        BallotNum ballot = Types.BallotNum.of(msg.getBallot());

        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.NEW_VIEW,
                msg.getBallot().getNodeId(), msg.getBallot(), null, null, "accepts=" + msg.getAcceptsCount());
        if (msg.getCheckpointSeq() > 0) {
            handleCheckpointMetadata(msg.getCheckpointSeq(), msg.getCheckpointDigest(), leaderIdNv);
        }
        state.learnLeader(ballot, msg.getBallot().getNodeId());
        electionCoordinator.viewHistory().add(msg);
        log("NEW-VIEW(acks) apply ballot=%s accepts=%d", ballotString(ballot), msg.getAcceptsCount());
        electionCoordinator.noteBallotSeen(ballot);
        electionCoordinator.gotLeaderMessage();

        NewViewReply.Builder reply = NewViewReply.newBuilder();
        Set<Integer> acceptedSeqs = new HashSet<>();
        int maxSeq = 0;
        int self = state.getNodeConfig().getNodeId();
        for (AcceptMsg a : msg.getAcceptsList()) {
            if (a.getSeq() <= msg.getCheckpointSeq()) {
                Accepted ok = Accepted.newBuilder()
                        .setB(a.getBallot())
                        .setSeq(a.getSeq())
                        .setFromNode(self)
                        .setOk(true)
                        .build();
                MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                        leaderIdNv, ok.getB(), ok.getSeq(), null, "ok new-view cp-skip");
                reply.addAcks(ok);
                continue;
            }
            maxSeq = Math.max(maxSeq, a.getSeq());
            acceptedSeqs.add(a.getSeq());
            BallotNum acceptBallot = Types.BallotNum.of(a.getBallot());
            boolean canAccept = state.acceptBallot(acceptBallot);
            if (canAccept) {
                var le = state.getLog().computeIfAbsent(a.getSeq(),
                        s -> new Types.LogEntry(s, a.getReq(), acceptBallot));
                if (le.getPhase().ordinal() >= Phase.ACCEPTED.ordinal()
                        && le.getBallot() != null && acceptBallot != null
                        && le.getBallot().equals(acceptBallot)
                        && !java.util.Objects.equals(le.getReq(), a.getReq())) {
                    Accepted rejected = Accepted.newBuilder()
                            .setB(a.getBallot())
                            .setSeq(a.getSeq())
                            .setFromNode(self)
                            .setOk(false)
                            .setReason("Conflicting value for same ballot/seq")
                            .build();
                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                            leaderIdNv, rejected.getB(), rejected.getSeq(), null, "nack new-view: Conflicting value for same ballot/seq");
                    reply.addAcks(rejected);
                    continue;
                }
                if (le.getPhase() != Phase.EXECUTED) {
                    le.setReq(a.getReq());
                    le.setBallot(acceptBallot);
                    le.setTwoPcTag(a.getTwoPcTag());
                    le.advancePhase(Phase.ACCEPTED);
                }
                Accepted ok = Accepted.newBuilder()
                        .setB(a.getBallot())
                        .setSeq(a.getSeq())
                        .setFromNode(self)
                        .setOk(true)
                        .build();
                MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                        leaderIdNv, ok.getB(), ok.getSeq(), null, "ok new-view");
                reply.addAcks(ok);
            } else {
                BallotNum hint = state.getHighestSeenBallot();
                Accepted nack = Accepted.newBuilder()
                        .setB(hint != null ? hint.toProto() : a.getBallot())
                        .setSeq(a.getSeq())
                        .setFromNode(self)
                        .setOk(false)
                        .setReason("Higher ballot promised")
                        .build();
                MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPTED,
                        leaderIdNv, nack.getB(), nack.getSeq(), null, "nack new-view: Higher ballot promised");
                reply.addAcks(nack);
            }
        }
        if (maxSeq > 0) {
            state.advanceNextSeqIfNeeded(maxSeq);
        }

        int pruned = state.pruneLogAfterNewView(msg.getCheckpointSeq(), acceptedSeqs);
        if (pruned > 0) {
            log("NEW-VIEW(acks) pruned %d unapplied entries beyond checkpoint=%d", pruned, msg.getCheckpointSeq());
        }

        electionCoordinator.gotLeaderMessage();

        resp.onNext(reply.build());
        resp.onCompleted();
    }

    @Override
    public void checkpoint(CheckpointMsg msg, StreamObserver<Empty> resp) {
        if (state.isLfSuspended()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        int seq = msg.getSeq();
        String digest = msg.getDigest();
        int sourceId = msg.getSourceNodeId();
        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.CHECKPOINT,
                sourceId, null, seq, null, "digest=" + (digest == null ? "" : digest));

        if (!checkpointingEnabled) {
            log("CHECKPOINT ignored (feature disabled) seq=%d from=%d", seq, sourceId);
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }

        handleCheckpointMetadata(seq, digest, sourceId);

        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void printView(Empty e, StreamObserver<ViewDump> resp) {
        ViewDump.Builder b = ViewDump.newBuilder();
        for (NewViewMsg nv : electionCoordinator.viewHistory()) b.addViews(nv);
        resp.onNext(b.build());
        resp.onCompleted();
    }


    @Override
    public void setLivePeers(PaxosProto.LivePeers req,
                             StreamObserver<PaxosProto.Empty> resp) {
        LinkedHashSet<Integer> peers = new LinkedHashSet<>(req.getNodeIdsList());
        Set<Integer> newlyReachable = state.setLivePeerIds(peers);
        log("ADMIN setLivePeers -> %s", peers.isEmpty() ? "ALL" : peers);

        if (state.getKnownLeaderId() == 0 && state.isSelfAllowed()) {
            log("ADMIN setLivePeers -> no leader in filter; probing election soon");
            electionCoordinator.probeElectionSoon();
        }
        if (state.getIsLeader() && !newlyReachable.isEmpty()) {
            catchUpNewPeers(newlyReachable);
        }
        resp.onNext(PaxosProto.Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void applyShardDelta(ShardDelta msg, StreamObserver<Empty> resp) {
        if (msg == null) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        if (!state.isSelfAllowed()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }

        java.util.Map<String,Integer> assign = new java.util.HashMap<>();
        for (KV kv : msg.getAssignList()) {
            String key = kv.getKey();
            if (key == null) {
                continue;
            }
            String trimmed = key.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            assign.put(trimmed, kv.getValue());
        }

        java.util.Set<String> drop = new java.util.LinkedHashSet<>();
        for (String raw : msg.getDropList()) {
            if (raw == null) {
                continue;
            }
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            drop.add(trimmed);
        }

        try {
            state.applyAccountOwnershipDelta(assign, drop);
        } catch (Exception ex) {
            log("ADMIN applyShardDelta failed: %s", ex.getMessage());
        }

        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void twoPcPrepare(TwoPcPrepareMsg msg, StreamObserver<TwoPcPrepareReply> resp) {
        TwoPcPrepareReply.Builder out = TwoPcPrepareReply.newBuilder();

        if (state.isLfSuspended()) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("Leader suspended for current set");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }
        if (!state.isSelfAllowed()) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("Node not live");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        if (!state.getIsLeader()) {
            int leaderId = state.getKnownLeaderId();
            if (leaderId != 0 && state.isPeerAllowed(leaderId)) {
                NodeServiceGrpc.NodeServiceBlockingStub leaderStub = stubs.get(leaderId);
                if (leaderStub != null) {
                    try {
                        TwoPcPrepareReply fwd = leaderStub
                                .withDeadlineAfter(FOLLOWER_FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .twoPcPrepare(msg);
                        resp.onNext(fwd);
                        resp.onCompleted();
                        return;
                    } catch (Exception ignored) { }
                }
            }
            // Fall back to local handling if leader is unknown or forwarding fails.
        }

        ClientRequest req = (msg == null ? null : msg.getReq());
        if (req == null || req.getClientId().isEmpty() || req.getTimestamp() <= 0L) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("Invalid 2PC prepare request");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        if (!isWriteTxn(req)) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("2PC prepare only valid for write transactions");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        String txnId = Types.txnId(req);
        if (txnId == null) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("Invalid 2PC txn id");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        int rId = parseAccountId(req.getReceiver());
        if (rId <= 0) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("Invalid 2PC receiver account");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        if (!ownsAccountLocally(req.getReceiver())) {
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("2PC prepare not responsible for receiver");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        TwoPcState txState = state.getTwoPcStates().computeIfAbsent(txnId,
                id -> new TwoPcState(id, req));


        if (txState.hasFinalDecision()) {
            TwoPcPrepareReply.PrepareOutcome outcome =
                    txState.isCommitDecision()
                            ? TwoPcPrepareReply.PrepareOutcome.PREPARED
                            : TwoPcPrepareReply.PrepareOutcome.ABORTED;
            out.setSuccess(true)
                    .setOutcome(outcome)
                    .setMessage("2PC prepare replay after final decision");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        if (txState.isLocalPrepared()) {
            out.setSuccess(true)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.PREPARED)
                    .setMessage("2PC already prepared locally");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        txState.setRole(TwoPcRole.PARTICIPANT);
        txState.setPhase(TwoPcPhase.PREPARING);

        java.util.List<String> lockedKeys = txState.getLockedKeys();
        if (lockedKeys == null || lockedKeys.isEmpty()) {
            java.util.List<String> toLock = new java.util.ArrayList<>(1);
            toLock.add(req.getReceiver());

            // Try to lock once - no wait loop, fail immediately on lock contention
            lockedKeys = state.tryLockAccounts(toLock);

            if (lockedKeys == null || lockedKeys.isEmpty()) {
                txState.decideCommit(false);
                runTwoPcPaxosRound(req, TwoPcTag.ABORT, txState);
                out.setSuccess(true)
                        .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                        .setMessage("2PC aborted due to lock contention on receiver");
                resp.onNext(out.build());
                resp.onCompleted();
                return;
            }

            txState.setLockedKeys(lockedKeys);
        }



        Boolean localPrepared = runTwoPcPaxosRound(req, TwoPcTag.PREPARE, txState);
        if (localPrepared == null) {


            txState.touchForTimer();
            state.unlockAccounts(lockedKeys);
            txState.setLockedKeys(null);
            out.setSuccess(false)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("2PC aborted: PREPARE quorum failed on participant shard (retry)");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }
        if (!localPrepared) {


            txState.decideCommit(false);
            state.unlockAccounts(lockedKeys);
            txState.setLockedKeys(null);
            out.setSuccess(true)
                    .setOutcome(TwoPcPrepareReply.PrepareOutcome.ABORTED)
                    .setMessage("2PC aborted: local PREPARE failed on participant shard");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        txState.markLocalPrepared();
        out.setSuccess(true)
                .setOutcome(TwoPcPrepareReply.PrepareOutcome.PREPARED)
                .setMessage("2PC prepare succeeded; receiver locked");
        resp.onNext(out.build());
        resp.onCompleted();
    }

    @Override
    public void twoPcDecide(TwoPcDecideMsg msg, StreamObserver<TwoPcDecideReply> resp) {
        TwoPcDecideReply.Builder out = TwoPcDecideReply.newBuilder();

        if (state.isLfSuspended()) {
            out.setSuccess(false)
                    .setMessage("Leader suspended for current set");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }
        if (!state.isSelfAllowed()) {
            out.setSuccess(false)
                    .setMessage("Node not live");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        if (!state.getIsLeader()) {
            int leaderId = state.getKnownLeaderId();
            if (leaderId != 0 && state.isPeerAllowed(leaderId)) {
                NodeServiceGrpc.NodeServiceBlockingStub leaderStub = stubs.get(leaderId);
                if (leaderStub != null) {
                    try {
                        TwoPcDecideReply fwd = leaderStub
                                .withDeadlineAfter(FOLLOWER_FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .twoPcDecide(msg);
                        resp.onNext(fwd);
                        resp.onCompleted();
                        return;
                    } catch (Exception ignored) { }
                }
            }
            // Fall back to local decide if leader is unknown or forwarding fails.
        }

        ClientRequest req = (msg == null ? null : msg.getReq());
        if (req == null || req.getClientId().isEmpty() || req.getTimestamp() <= 0L) {
            out.setSuccess(false)
                    .setMessage("Invalid 2PC decide request");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        String txnId = Types.txnId(req);
        if (txnId == null) {
            out.setSuccess(false)
                    .setMessage("Invalid 2PC txn id in decide");
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        TwoPcState txState = state.getTwoPcStates().get(txnId);
        boolean commit = (msg.getDecision() == TwoPcDecideMsg.Decision.COMMIT);

        if (txState == null) {
            if (!commit) {
                out.setSuccess(true)
                        .setMessage("2PC decide ABORT for unknown txn; treated as no-op");
            } else {
                out.setSuccess(false)
                        .setMessage("2PC decide COMMIT for unknown txn; cannot apply");
            }
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }

        if (txState.hasFinalDecision()) {
            boolean prevCommit = txState.isCommitDecision();
            if (prevCommit == commit) {
                out.setSuccess(true)
                        .setMessage("2PC decide replay; decision already applied");
            } else {
                out.setSuccess(false)
                        .setMessage("Conflicting 2PC decide for txn");
            }
            resp.onNext(out.build());
            resp.onCompleted();
            return;
        }




        TwoPcTag tag = commit ? TwoPcTag.COMMIT : TwoPcTag.ABORT;
        log("2PC PARTICIPANT DECIDE recv txnId=%s decision=%s", txnId, (commit ? "COMMIT" : "ABORT"));
        Boolean exec = runTwoPcPaxosRound(req, tag, txState);

        if (exec == null || !exec) {




            log("2PC PARTICIPANT DECIDE txnId=%s tag=%s execResult=%s; keeping locks", txnId, tag.name(), exec);
            out.setSuccess(false)
                    .setMessage(commit
                            ? "2PC commit Paxos failed locally"
                            : "2PC abort Paxos failed locally");
        } else {
            java.util.List<String> lockedKeys = txState.getLockedKeys();
            if (lockedKeys != null && !lockedKeys.isEmpty()) {
                state.unlockAccounts(lockedKeys);
                txState.setLockedKeys(null);
            }
            log("2PC PARTICIPANT DECIDE applied=%s txnId=%s; locksReleased=%s",
                    commit ? "COMMIT" : "ABORT", txnId,
                    (txState.getLockedKeys() == null || txState.getLockedKeys().isEmpty()));
            out.setSuccess(true)
                    .setMessage(commit
                            ? "2PC commit applied locally via Paxos (state updated and locks released)"
                            : "2PC abort applied locally via Paxos (state reverted and locks released)");
        }
        resp.onNext(out.build());
        resp.onCompleted();
    }

    @Override
    public void resetView(Empty e, StreamObserver<Empty> resp) {
        log("ADMIN resetView -> resetting view history and node state for new set");
        electionCoordinator.resetViewHistory();
        state.resetForNewSet();
        log("ADMIN resetView -> completed resetForNewSet on node %d", state.getNodeConfig().getNodeId());
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void suspendForSet(Empty e, StreamObserver<Empty> resp) {
        if (state.getIsLeader() && state.isSelfAllowed()) {
            state.setLfSuspended(true);
            log("ADMIN LF -> leader suspended for current set");
            state.setTimersPaused(true);
        }
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void resumeAfterSet(Empty e, StreamObserver<Empty> resp) {
        state.setLfSuspended(false);
        log("ADMIN resume-after-set -> clear LF suspension");

        state.setTimersPaused(false);
        electionCoordinator.probeElectionSoon();
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void pauseTimers(Empty e, StreamObserver<Empty> resp) {
        state.setTimersPaused(true);
        log("ADMIN timers paused");

        if (state.getIsLeader() && state.isSelfAllowed()) {
            int selfId = state.getNodeConfig().getNodeId();
            java.util.Set<Integer> peersToCatch = stubs.keySet().stream()
                    .filter(pid -> pid != selfId && state.isPeerAllowed(pid))
                    .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
            if (!peersToCatch.isEmpty()) {
                catchUpNewPeers(peersToCatch);
            }
        }
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void resumeTimers(Empty e, StreamObserver<Empty> resp) {
        state.setTimersPaused(false);
        electionCoordinator.gotLeaderMessage();
        log("ADMIN timers resumed");
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }


    @Override
    public void heartbeat(HeartbeatMsg msg, StreamObserver<Empty> resp) {
        if (state.isLfSuspended()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        if (!state.isSelfAllowed()) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        int leaderId = msg.getNodeId();
        if (leaderId != 0 && !state.isPeerAllowed(leaderId)) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        BallotNum hb = msg.hasBallot() ? BallotNum.of(msg.getBallot()) : null;
        BallotNum promised = state.getHighestSeenBallot();

        //log("HEARTBEAT recv from=%d ballot=%s promised=%s", leaderId, ballotString(hb), ballotString(promised));


        if (hb == null) {
            log("HEARTBEAT ignored (no ballot) from=%d", leaderId);
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }


        state.learnLeader(hb, leaderId);
        electionCoordinator.noteBallotSeen(hb);
        electionCoordinator.gotLeaderMessage();
        //log("HEARTBEAT fresh from=%d hb=%s (promised=%s) -> learned+reset", leaderId, ballotString(hb), ballotString(promised));
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }


    private void catchUpNewPeers(Set<Integer> peerIds) {
        if (!state.getIsLeader() || !state.isSelfAllowed()) {
            return;
        }
        if (peerIds == null || peerIds.isEmpty()) {
            return;
        }
        List<Types.LogEntry> committed = state.getLog().values().stream()
                .filter(le -> le.getPhase().ordinal() >= Phase.COMMITTED.ordinal())
                .sorted(Comparator.comparingInt(Types.LogEntry::getSeq))
                .collect(Collectors.toList());
        if (committed.isEmpty()) {
            return;
        }
        for (int peerId : peerIds) {
            if (!state.getIsLeader()) {
                log("catch-up aborted: lost leadership");
                return;
            }
            if (peerId == state.getNodeConfig().getNodeId()) {
                continue;
            }
            NodeServiceGrpc.NodeServiceBlockingStub stub = stubs.get(peerId);
            if (stub == null) {
                continue;
            }
            int cp = state.getLastCheckpointSeq();
            String digest = state.getLastCheckpointDigest();
            if (checkpointingEnabled && cp > 0) {
                try {
                    CheckpointMsg.Builder cb = CheckpointMsg.newBuilder()
                            .setSeq(cp)
                            .setSourceNodeId(state.getNodeConfig().getNodeId());
                    if (digest != null && !digest.isEmpty()) {
                        cb.setDigest(digest);
                    }
                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.CHECKPOINT,
                            peerId, null, cp, null, "catch-up");
                    stub.checkpoint(cb.build());
                } catch (Exception ex) {
                    log("catch-up checkpoint -> n%d failed at seq=%d: %s", peerId, cp, ex.getMessage());
                }
            }

            List<Types.LogEntry> sweep = committed;
            log("catch-up -> n%d entries=%d", peerId, sweep.size());
            boolean complete = true;
            for (Types.LogEntry entry : sweep) {
                if (!state.getIsLeader()) { complete = false; break; }
                ClientRequest req = entry.getReq();
                if (req == null) {
                    req = Types.NOOP;
                }
                CommitMsg commit = CommitMsg.newBuilder()
                        .setBallot(entry.getBallot() != null ? entry.getBallot().toProto() : PaxosProto.Ballot.getDefaultInstance())
                        .setSeq(entry.getSeq())
                        .setReq(req)
                        .setTwoPcTag(entry.getTwoPcTag())
                        .build();
                try {

                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.COMMIT,
                            peerId, commit.getBallot(), commit.getSeq(), commit.getReq(), "catch-up");
                    stub.commit(commit);
                } catch (Exception ex) {
                    log("catch-up -> n%d failed at seq=%d: %s", peerId, entry.getSeq(), ex.getMessage());
                    complete = false;
                    break;
                }
            }
            if (complete) {

                try {
                    stub.withDeadlineAfter(Math.max(FOLLOWER_FORWARD_TIMEOUT_MS, 500L), java.util.concurrent.TimeUnit.MILLISECONDS)
                            .getDB(Empty.getDefaultInstance());
                } catch (Exception ignored) { }
                log("catch-up -> n%d complete", peerId);
            }
        }
    }

    private void scanTwoPcTimeouts() {
        try {
            if (!state.getIsLeader()) {
                return;
            }
            if (state.isTimersPaused() || state.isLfSuspended() || !state.isSelfAllowed()) {
                return;
            }
            long now = System.nanoTime();
            for (Types.TwoPcState tx : state.getTwoPcStates().values()) {
                if (tx == null) continue;

                Types.TwoPcRole role;
                Types.TwoPcPhase phase;
                boolean finalKnown;
                long lastUpdateNs;
                boolean hasLogSeq;

                synchronized (tx) {
                    role = tx.getRole();
                    phase = tx.getPhase();
                    finalKnown = tx.hasFinalDecision();
                    lastUpdateNs = tx.getLastUpdateNs();
                    hasLogSeq = tx.hasLogSeq();
                }

                if (role != Types.TwoPcRole.COORDINATOR) {
                    continue;
                }
                if (tx.getLogSeq() <= 0) {
                    continue;
                }
                if (finalKnown) {
                    continue;
                }
                if (!hasLogSeq) {
                    continue;
                }

                long ageMs = (now - lastUpdateNs) / 1_000_000L;
                boolean shouldAbort = false;
                if (phase == Types.TwoPcPhase.PREPARING && ageMs > COORD_PREPARE_TIMEOUT_MS) {
                    shouldAbort = true;
                } else if (phase == Types.TwoPcPhase.PREPARED && ageMs > COORD_DECISION_TIMEOUT_MS) {
                    shouldAbort = true;
                }
                if (!shouldAbort) {
                    continue;
                }

                ClientRequest req;
                synchronized (tx) {
                    if (tx.hasFinalDecision()) {
                        continue;
                    }
                    Types.TwoPcPhase curPhase = tx.getPhase();
                    if (curPhase != Types.TwoPcPhase.PREPARING && curPhase != Types.TwoPcPhase.PREPARED) {
                        continue;
                    }
                    req = tx.getRequest();
                }
                if (req == null) {
                    continue;
                }

                Boolean abortRes = runTwoPcPaxosRound(req, TwoPcTag.ABORT, tx);
                if (!java.lang.Boolean.TRUE.equals(abortRes)) {
                    continue;
                }

                java.util.List<String> lockedKeys;
                synchronized (tx) {
                    if (!tx.hasFinalDecision()) {
                        tx.decideCommit(false);
                    }
                    lockedKeys = tx.getLockedKeys();
                }
                if (lockedKeys != null && !lockedKeys.isEmpty()) {
                    state.unlockAccounts(lockedKeys);
                    synchronized (tx) {
                        tx.setLockedKeys(null);
                    }
                }
            }
        } catch (Throwable t) {
            try {
                log("2PC timeout scan error: %s", t.toString());
            } catch (Throwable ignored) { }
        }
    }
    private void log(String fmt, Object... args) {
        PrintHelper.debugf(logPrefix + fmt, args);
    }

    private static String ballotString(BallotNum b) {
        if (b == null) return "(?,?)";
        return "(" + b.getRound() + "," + b.getNodeId() + ")";
    }

    private boolean isWriteTxn(ClientRequest req) {
        if (req == null) return false;
        if (req.getAmount() <= 0) return false;
        String s = req.getSender();
        String r = req.getReceiver();
        return s != null && r != null && !s.isEmpty() && !r.isEmpty();
    }

    private boolean isReadOnlyTxn(ClientRequest req) {
        if (req == null) return false;
        if (req.getAmount() != 0) return false;
        String s = req.getSender();
        String r = req.getReceiver();
        return s != null && !s.isEmpty() && (r == null || r.isEmpty());
    }

    private int parseAccountId(String s) {
        if (s == null) return -1;
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return -1;
        }
    }

    private boolean ownsAccountLocally(String rawId) {
        if (rawId == null) return false;
        String trimmed = rawId.trim();
        if (trimmed.isEmpty()) return false;
        Integer bal = state.getDatabase().getBalance(trimmed);
        return bal != null;
    }

    private int clusterForAccount(int id) {
        if (id >= 1 && id <= 3000) return 1;
        if (id >= 3001 && id <= 6000) return 2;
        if (id >= 6001 && id <= 9000) return 3;
        return 0;
    }

    private int clusterForNode(int nodeId) {
        if (nodeId >= 1 && nodeId <= 3) return 1;
        if (nodeId >= 4 && nodeId <= 6) return 2;
        if (nodeId >= 7 && nodeId <= 9) return 3;
        return 0;
    }

    private boolean isIntraShardWrite(ClientRequest req) {
        if (!isWriteTxn(req)) return false;
        boolean senderLocal = ownsAccountLocally(req.getSender());
        boolean receiverLocal = ownsAccountLocally(req.getReceiver());
        return senderLocal && receiverLocal;
    }

    private boolean isCrossShardWriteOnSenderLeader(ClientRequest req) {
        if (!isWriteTxn(req)) return false;
        boolean senderLocal = ownsAccountLocally(req.getSender());
        boolean receiverLocal = ownsAccountLocally(req.getReceiver());
        return senderLocal && !receiverLocal;
    }

    private NodeServiceGrpc.NodeServiceBlockingStub selectTwoPcParticipantStub(int targetCluster) {
        if (twoPcStubs == null || twoPcStubs.isEmpty()) return null;
        NodeServiceGrpc.NodeServiceBlockingStub chosen = null;
        int bestNodeId = Integer.MAX_VALUE;
        for (Map.Entry<Integer, NodeServiceGrpc.NodeServiceBlockingStub> e : twoPcStubs.entrySet()) {
            int nodeId = e.getKey();
            if (clusterForNode(nodeId) != targetCluster) continue;


            if (!state.isPeerAllowed(nodeId)) continue;
            if (nodeId < bestNodeId) {
                bestNodeId = nodeId;
                chosen = e.getValue();
            }
        }
        return chosen;
    }

    private void sendTwoPcDecideWithRetries(NodeServiceGrpc.NodeServiceBlockingStub stub,
                                            ClientRequest req,
                                            TwoPcDecideMsg.Decision decision) {
        if (stub == null || req == null || decision == null) {
            return;
        }
        Integer prop = Integer.getInteger("twoPcDecideMaxAttempts", 0);
        int maxAttempts = (prop != null ? prop : 0);



        long baseBackoffMs = Long.getLong("twoPcDecideBackoffMs", 100L);

        for (int attempt = 1; ; attempt++) {
            try {
                TwoPcDecideMsg decide = TwoPcDecideMsg.newBuilder()
                        .setReq(req)
                        .setDecision(decision)
                        .build();
                TwoPcDecideReply reply = stub
                        .withDeadlineAfter(TWO_PC_RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .twoPcDecide(decide);
                if (reply.getSuccess()) {
                    break;
                }
            } catch (Exception ex) {

            }


            if (maxAttempts > 0 && attempt >= maxAttempts) {
                break;
            }



            if (baseBackoffMs > 0L) {
                long sleepMs = baseBackoffMs * (long) Math.min(attempt, 10);
                sleepMs = Math.max(10L, Math.min(sleepMs, 1000L));
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private Reply handleClientSubmit(ClientRequest req) {

        ClientState cs = state.getClientStates().computeIfAbsent(req.getClientId(), k -> new Types.ClientState());

        synchronized (cs) {
            if (req.getTimestamp() < cs.getLastTs() && cs.getLastReply() != null) {
                return cs.getLastReply();
            }
            if (req.getTimestamp() == cs.getLastTs() && cs.getLastReply() != null) {
                return cs.getLastReply();
            }
        }

        Ballot leaderBallot = state.getCurrentBallot() != null ? state.getCurrentBallot().toProto() : Ballot.getDefaultInstance();

        log("CLIENT SUBMIT recv ts=%d sender=%s receiver=%s amt=%d selfNode=%d isLeader=%s",
                req.getTimestamp(), req.getSender(), req.getReceiver(), req.getAmount(),
                state.getNodeConfig().getNodeId(), state.getIsLeader());

        if (!state.getIsLeader()) {
            if (state.getKnownLeaderId() == 0) {
                electionCoordinator.probeElectionSoon();
            }
            NodeServiceGrpc.NodeServiceBlockingStub leaderStub = null;
            int leaderId = state.getKnownLeaderId();
            if (leaderId != 0 && state.isPeerAllowed(leaderId)) leaderStub = stubs.get(leaderId);
            Reply r;
            try {
                if (leaderStub == null) {
                    log("CLIENT redirect unknown leader for txn %s->%s amt=%d", req.getSender(), req.getReceiver(), req.getAmount());
                    r = Reply.newBuilder().setSuccess(false).setMessage("Redirect: unknown leader")
                            .setTimestamp(req.getTimestamp()).setLeaderBallot(leaderBallot)
                            .setResultType(Reply.ResultType.OTHER).build();
                } else {
                    r = leaderStub.withDeadlineAfter(FOLLOWER_FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS).submit(req);
                }
            } catch (Exception e) {
                log("CLIENT leader %d unreachable txn %s->%s amt=%d", leaderId, req.getSender(), req.getReceiver(), req.getAmount());
                r = Reply.newBuilder().setSuccess(false).setMessage("Leader unreachable")
                        .setTimestamp(req.getTimestamp()).setLeaderBallot(leaderBallot)
                        .setResultType(Reply.ResultType.OTHER).build();
            }

            return r;
        }

        synchronized (cs) {
            if (req.getTimestamp() < cs.getLastTs() && cs.getLastReply() != null) {
                return cs.getLastReply();
            }
            if (req.getTimestamp() == cs.getLastTs() && cs.getLastReply() != null) {
                return cs.getLastReply();
            }
            if (cs.getPendingTs() == req.getTimestamp()) {
                return Reply.newBuilder()
                        .setSuccess(false)
                        .setResultType(Reply.ResultType.OTHER)
                        .setMessage("Duplicate pending")
                        .setTimestamp(req.getTimestamp())
                        .setLeaderBallot(leaderBallot)
                        .build();
            }
            cs.setPendingTs(req.getTimestamp());
        }

        if (electionCoordinator.isRecovering()) {
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Leader recovering")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }

        if (isReadOnlyTxn(req)) {
            flushStateMachine();
            Integer bal;
            if (PERFORMANCE_MODE) {
                bal = state.getDatabase().getBalance(req.getSender());
            } else {
                java.util.Map<String, Integer> snap = state.getDatabase().snapshot();
                bal = snap.get(req.getSender());
            }
            boolean found = (bal != null);
            Reply reply = Reply.newBuilder()
                    .setSuccess(found)
                    .setMessage(found ? Integer.toString(bal) : "Account not found")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(found ? Reply.ResultType.SUCCESS : Reply.ResultType.FAILURE)
                    .build();
            cacheReplyIfFinal(cs, reply);
            return reply;
        }

        boolean isWrite = isWriteTxn(req);
        java.util.List<String> lockedKeys = null;

        if (isWrite) {
            if (isIntraShardWrite(req)) {
                java.util.List<String> toLock = new java.util.ArrayList<>(2);
                toLock.add(req.getSender());
                toLock.add(req.getReceiver());


                // Try to lock once - no wait loop, fail immediately on lock contention
                lockedKeys = state.tryLockAccounts(toLock);

                if (lockedKeys == null || lockedKeys.isEmpty()) {
                    // Per spec: lock contention is final, return FAILURE
                    synchronized (cs) { cs.clearPending(); }
                    Reply reply = Reply.newBuilder()
                            .setSuccess(false)
                            .setMessage("Skipped due to lock contention")
                            .setTimestamp(req.getTimestamp())
                            .setLeaderBallot(leaderBallot)
                            .setResultType(Reply.ResultType.FAILURE)
                            .build();
                    cacheReplyIfFinal(cs, reply);
                    return reply;
                }
            } else if (isCrossShardWriteOnSenderLeader(req)) {
                return handleCrossShardTwoPc(req, cs, leaderBallot);
            } else {
                int sId = parseAccountId(req.getSender());
                int rId = parseAccountId(req.getReceiver());
                int sCluster = clusterForAccount(sId);
                int rCluster = clusterForAccount(rId);
                int selfCluster = clusterForNode(state.getNodeConfig().getNodeId());
                log("WRITE gating failure: not intra-shard and not cross-shard-on-sender-leader ts=%d txn %s->%s amt=%d selfCluster=%d sCluster=%d rCluster=%d",
                        req.getTimestamp(), req.getSender(), req.getReceiver(), req.getAmount(),
                        selfCluster, sCluster, rCluster);
                synchronized (cs) { cs.clearPending(); }
                return Reply.newBuilder()
                        .setSuccess(false)
                        .setMessage("Cross-shard request routed to wrong shard")
                        .setTimestamp(req.getTimestamp())
                        .setLeaderBallot(leaderBallot)
                        .setResultType(Reply.ResultType.OTHER)
                        .build();
            }
        }

        int seq;
        Types.LogEntry logEntry;

        Integer existingSeq = findExistingSeqForRequest(req);
        if (existingSeq != null) {
            seq = existingSeq;
            logEntry = state.getLog().computeIfAbsent(seq,
                    s -> new Types.LogEntry(s, req, state.getCurrentBallot()));
        } else {
            seq = state.getAndIncrNextSeq();
            logEntry = state.getLog().computeIfAbsent(seq,
                    s -> new Types.LogEntry(s, req, state.getCurrentBallot()));
        }

        logEntry.setReq(req);
        logEntry.setBallot(state.getCurrentBallot());
        if (logEntry.getPhase().ordinal() < Phase.ACCEPTED.ordinal()) {
            logEntry.advancePhase(Phase.ACCEPTED);
        }

        if (isWrite && lockedKeys != null && !lockedKeys.isEmpty()) {
            state.registerLocksForSeq(seq, lockedKeys);
        }

        log("PROPOSE seq=%d ballot=%s txn %s->%s amt=%d", seq, ballotString(state.getCurrentBallot()), req.getSender(), req.getReceiver(), req.getAmount());
        AcceptMsg accept = AcceptMsg.newBuilder()
                .setBallot(state.getCurrentBallot().toProto())
                .setSeq(seq)
                .setReq(req)
                .setTwoPcTag(logEntry.getTwoPcTag())
                .build();
        boolean success = replication.sendAccept(accept);
        if (!success) {
            log("PROPOSE seq=%d quorum-failed", seq);
            state.unlockBySeq(seq);
            synchronized (cs) { cs.clearPending(); }

            return Reply.newBuilder().setSuccess(false).setMessage("No quorum: try again")
                    .setTimestamp(req.getTimestamp()).setLeaderBallot(state.getCurrentBallot().toProto())
                    .setResultType(Reply.ResultType.OTHER).build();
        }

        logEntry.advancePhase(Phase.COMMITTED);
        CommitMsg commit = CommitMsg.newBuilder()
                .setBallot(state.getCurrentBallot().toProto())
                .setSeq(seq)
                .setReq(req)
                .setTwoPcTag(logEntry.getTwoPcTag())
                .build();
        replication.sendCommit(commit);

        stateMachine.executeStateMachine();

        log("COMMIT broadcast seq=%d", seq);

        Boolean execSuccess = null;
        try {
            execSuccess = logEntry.getExecResult().get(3, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
        }

        if (execSuccess == null) {
            log("RESULT seq=%d pending execution", seq);
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Pending; retry")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(state.getCurrentBallot().toProto())
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }

        boolean executed = execSuccess;
        log("RESULT seq=%d executed=%s", seq, executed);
        Reply reply = Reply.newBuilder()
                .setSuccess(executed)
                .setMessage(executed ? "OK" : "Insufficient funds")
                .setTimestamp(req.getTimestamp())
                .setLeaderBallot(state.getCurrentBallot().toProto())
                .setResultType(executed ? Reply.ResultType.SUCCESS : Reply.ResultType.FAILURE)
                .build();

        cacheReplyIfFinal(cs, reply);
        return reply;
    }


    private Reply handleCrossShardTwoPc(ClientRequest req, ClientState cs, Ballot leaderBallot) {
        String txnId = Types.txnId(req);
        if (txnId == null) {
            synchronized (cs) { cs.clearPending(); }
            log("2PC COORD txnId null; invalid 2PC transaction id");
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Invalid 2PC transaction id")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }

        int sId = parseAccountId(req.getSender());
        int rId = parseAccountId(req.getReceiver());
        if (sId <= 0 || rId <= 0) {
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Invalid account id for 2PC")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }
        if (!ownsAccountLocally(req.getSender())) {
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Cross-shard request routed to wrong shard for sender")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }
        if (ownsAccountLocally(req.getReceiver())) {
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Request no longer cross-shard; retry")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }

        TwoPcState txState = state.getTwoPcStates().computeIfAbsent(txnId,
                id -> new TwoPcState(id, req));
        txState.setRole(TwoPcRole.COORDINATOR);

        log("2PC COORD begin txnId=%s sender=%s receiver=%s amt=%d",
                txnId, req.getSender(), req.getReceiver(), req.getAmount());

        if (txState.hasFinalDecision()) {
            log("2PC COORD txnId=%s hasFinalDecision commit=%s", txnId, txState.isCommitDecision());
            boolean commit = txState.isCommitDecision();
            Reply reply = Reply.newBuilder()
                    .setSuccess(commit)
                    .setMessage(commit ? "OK" : "Transaction aborted")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(commit ? Reply.ResultType.SUCCESS : Reply.ResultType.FAILURE)
                    .build();
            cacheReplyIfFinal(cs, reply);
            return reply;
        }

        java.util.List<String> lockedKeys = txState.getLockedKeys();
        if (lockedKeys == null || lockedKeys.isEmpty()) {
            java.util.List<String> toLock = new java.util.ArrayList<>(1);
            toLock.add(req.getSender());


            // Try to lock once - no wait loop, fail immediately on lock contention
            lockedKeys = state.tryLockAccounts(toLock);

            if (lockedKeys == null || lockedKeys.isEmpty()) {
                // Per spec: lock contention is final, abort and return FAILURE
                log("2PC COORD txnId=%s sender lock contention; aborting", txnId);
                txState.decideCommit(false);
                synchronized (cs) { cs.clearPending(); }
                Reply reply = Reply.newBuilder()
                        .setSuccess(false)
                        .setMessage("Skipped due to lock contention on sender")
                        .setTimestamp(req.getTimestamp())
                        .setLeaderBallot(leaderBallot)
                        .setResultType(Reply.ResultType.FAILURE)
                        .build();
                cacheReplyIfFinal(cs, reply);
                return reply;
            }

            txState.setLockedKeys(lockedKeys);
        }

        Integer oldBal = state.getDatabase().getBalance(req.getSender());
        if (oldBal == null || oldBal < req.getAmount()) {
            log("2PC COORD txnId=%s insufficient funds: bal=%s amt=%d", txnId, oldBal, req.getAmount());
            txState.decideCommit(false);
            state.unlockAccounts(lockedKeys);
            txState.setLockedKeys(null);
            Reply reply = Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Insufficient funds")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.FAILURE)
                    .build();
            cacheReplyIfFinal(cs, reply);
            return reply;
        }

        txState.setPhase(TwoPcPhase.PREPARING);

        // Run local Paxos PREPARE first - timing differences naturally prevent deadlock
        // in bidirectional transactions (one completes before the other's participant phase)
        Boolean localPrepared = runTwoPcPaxosRound(req, TwoPcTag.PREPARE, txState);
        if (localPrepared == null) {
            txState.touchForTimer();
            state.unlockAccounts(lockedKeys);
            txState.setLockedKeys(null);
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("2PC local PREPARE pending; retry")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }
        if (!localPrepared) {
            log("2PC COORD txnId=%s local PREPARE failed; aborting before participant", txnId);
            txState.decideCommit(false);
            state.unlockAccounts(lockedKeys);
            txState.setLockedKeys(null);
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("2PC local prepare failed")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.FAILURE)
                    .build();
        }

        int selfCluster = clusterForNode(state.getNodeConfig().getNodeId());
        NodeServiceGrpc.NodeServiceBlockingStub participantStub = null;
        TwoPcPrepareReply prepReply = null;

        TwoPcPrepareMsg prep = TwoPcPrepareMsg.newBuilder().setReq(req).build();

        for (int candidateCluster = 1; candidateCluster <= 3; candidateCluster++) {
            if (candidateCluster == selfCluster) {
                continue;
            }

            NodeServiceGrpc.NodeServiceBlockingStub stub = selectTwoPcParticipantStub(candidateCluster);
            log("2PC COORD select participant cluster=%d stubNull=%s txn %s->%s amt=%d",
                    candidateCluster, (stub == null), req.getSender(), req.getReceiver(), req.getAmount());
            if (stub == null) {
                continue;
            }

            try {
                log("2PC COORD sending PREPARE to participant cluster=%d txn %s->%s amt=%d",
                        candidateCluster, req.getSender(), req.getReceiver(), req.getAmount());
                TwoPcPrepareReply r = stub
                        .withDeadlineAfter(TWO_PC_RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .twoPcPrepare(prep);
                log("2PC COORD PREPARE reply from participant cluster=%d: success=%s outcome=%s msg=%s txn %s->%s amt=%d",
                        candidateCluster, r.getSuccess(), r.getOutcome(), r.getMessage(),
                        req.getSender(), req.getReceiver(), req.getAmount());

                if (r.getSuccess()
                        && r.getOutcome() == TwoPcPrepareReply.PrepareOutcome.PREPARED) {
                    participantStub = stub;
                    prepReply = r;
                    break;
                }

                boolean notOwner = !r.getSuccess()
                        && r.getOutcome() == TwoPcPrepareReply.PrepareOutcome.ABORTED
                        && r.getMessage() != null
                        && r.getMessage().contains("not responsible for receiver");
                boolean participantQuorumFailed = r.getMessage() != null
                        && r.getMessage().contains("PREPARE quorum failed on participant shard");
                if (participantQuorumFailed) {
                    // Check retry count - abort if exceeds CLIENT_MAX_ATTEMPTS
                    int retries = txState.incrementParticipantRetryCount();
                    if (retries >= CLIENT_MAX_ATTEMPTS) {
                        // Max retries reached - abort
                        log("2PC COORD participant quorum failed (retries=%d); aborting txn %s->%s amt=%d",
                                retries, req.getSender(), req.getReceiver(), req.getAmount());
                        runTwoPcPaxosRound(req, TwoPcTag.ABORT, txState);
                        txState.decideCommit(false);
                        state.unlockAccounts(lockedKeys);
                        txState.setLockedKeys(null);
                        synchronized (cs) { cs.clearPending(); }
                        Reply reply = Reply.newBuilder()
                                .setSuccess(false)
                                .setMessage("2PC participant quorum failed; aborted")
                                .setTimestamp(req.getTimestamp())
                                .setLeaderBallot(leaderBallot)
                                .setResultType(Reply.ResultType.FAILURE)
                                .build();
                        cacheReplyIfFinal(cs, reply);
                        return reply;
                    }
                    // Participant quorum pending - allow retry (don't abort yet)
                    log("2PC COORD participant quorum pending (retry=%d); allowing retry txn %s->%s amt=%d",
                            retries, req.getSender(), req.getReceiver(), req.getAmount());
                    synchronized (cs) { cs.clearPending(); }
                    return Reply.newBuilder()
                            .setSuccess(false)
                            .setMessage("2PC participant quorum pending; retry")
                            .setTimestamp(req.getTimestamp())
                            .setLeaderBallot(leaderBallot)
                            .setResultType(Reply.ResultType.OTHER)
                            .build();
                }
                if (!notOwner) {
                    runTwoPcPaxosRound(req, TwoPcTag.ABORT, txState);
                    txState.decideCommit(false);
                    state.unlockAccounts(lockedKeys);
                    txState.setLockedKeys(null);
                    Reply reply = Reply.newBuilder()
                            .setSuccess(false)
                            .setMessage("2PC participant rejected: " + r.getMessage())
                            .setTimestamp(req.getTimestamp())
                            .setLeaderBallot(leaderBallot)
                            .setResultType(Reply.ResultType.FAILURE)
                            .build();
                    cacheReplyIfFinal(cs, reply);
                    return reply;
                }
            } catch (Exception e) {
                log("2PC COORD PREPARE RPC failed to candidate participant cluster=%d for txn %s->%s amt=%d ex=%s",
                        candidateCluster, req.getSender(), req.getReceiver(), req.getAmount(), e.toString());
            }
        }

        if (participantStub == null || prepReply == null) {
            // Check retry count - abort if exceeds CLIENT_MAX_ATTEMPTS
            int retries = txState.incrementParticipantRetryCount();
            if (retries >= CLIENT_MAX_ATTEMPTS) {
                log("2PC COORD participant unavailable (retries=%d); aborting txn %s->%s amt=%d",
                        retries, req.getSender(), req.getReceiver(), req.getAmount());
                runTwoPcPaxosRound(req, TwoPcTag.ABORT, txState);
                txState.decideCommit(false);
                state.unlockAccounts(lockedKeys);
                txState.setLockedKeys(null);
                synchronized (cs) { cs.clearPending(); }
                Reply reply = Reply.newBuilder()
                        .setSuccess(false)
                        .setMessage("Participant unavailable; aborted")
                        .setTimestamp(req.getTimestamp())
                        .setLeaderBallot(leaderBallot)
                        .setResultType(Reply.ResultType.FAILURE)
                        .build();
                cacheReplyIfFinal(cs, reply);
                return reply;
            }
            // Participant unavailable - allow retry (don't abort yet)
            log("2PC COORD participant unavailable (retry=%d); allowing retry txn %s->%s amt=%d",
                    retries, req.getSender(), req.getReceiver(), req.getAmount());
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("Participant unavailable; retry")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }

        txState.setPhase(TwoPcPhase.PREPARED);

        Boolean localCommit = null;
        int maxCommitAttempts = Integer.getInteger("twoPcCommitMaxAttempts", 5);
        if (maxCommitAttempts <= 0) {
            maxCommitAttempts = 5;
        }
        long commitBackoffMs = Long.getLong("twoPcCommitBackoffMs", 80L);

        for (int attempt = 1; attempt <= maxCommitAttempts; attempt++) {
            log("2PC COORD COMMIT attempt=%d txnId=%s", attempt, txnId);
            localCommit = runTwoPcPaxosRound(req, TwoPcTag.COMMIT, txState);
            log("2PC COORD COMMIT attempt=%d txnId=%s result=%s", attempt, txnId, localCommit);
            if (Boolean.TRUE.equals(localCommit)) {
                break;
            }


            if (Boolean.FALSE.equals(localCommit)) {
                break;
            }



            if (commitBackoffMs > 0L && attempt < maxCommitAttempts) {
                try {
                    Thread.sleep(commitBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (!Boolean.TRUE.equals(localCommit)) {
            // Treat quorum failure as in-doubt; do not flip to abort. Caller can retry.
            // Do NOT call resetLocalPrepareState - preserve TwoPcState so retries use same logSeq
            log("2PC COORD COMMIT failed txnId=%s localCommit=%s; leaving txn in-doubt for retry", txnId, localCommit);
            synchronized (cs) { cs.clearPending(); }
            return Reply.newBuilder()
                    .setSuccess(false)
                    .setMessage("2PC commit in-doubt; retry")
                    .setTimestamp(req.getTimestamp())
                    .setLeaderBallot(leaderBallot)
                    .setResultType(Reply.ResultType.OTHER)
                    .build();
        }

        sendTwoPcDecideWithRetries(participantStub, req, TwoPcDecideMsg.Decision.COMMIT);
        log("2PC COORD DECIDE COMMIT txnId=%s -> participant", txnId);

        txState.decideCommit(true);
        state.unlockAccounts(lockedKeys);
        txState.setLockedKeys(null);

        Reply reply = Reply.newBuilder()
                .setSuccess(true)
                .setMessage("OK")
                .setTimestamp(req.getTimestamp())
                .setLeaderBallot(leaderBallot)
                .setResultType(Reply.ResultType.SUCCESS)
                .build();
        cacheReplyIfFinal(cs, reply);
        return reply;
    }

    private Boolean runTwoPcPaxosRound(ClientRequest req, TwoPcTag tag, TwoPcState txState) {
        if (req == null || tag == null || tag == TwoPcTag.NONE || txState == null) {
            return null;
        }
        if (tag == TwoPcTag.PREPARE) {
            txState.touchForTimer();
        }
        // Per spec: PREPARE and COMMIT/ABORT are two separate consensus rounds with different sequence numbers
        int seq;
        if (tag == TwoPcTag.PREPARE) {
            seq = state.getOrAssignTwoPcSeq(txState);
        } else {
            // COMMIT or ABORT - use separate sequence number
            seq = state.getOrAssignTwoPcFinalSeq(txState);
        }
        Types.LogEntry logEntry = state.getLog().computeIfAbsent(seq,
                s -> new Types.LogEntry(s, req, state.getCurrentBallot()));
        logEntry.setReq(req);
        logEntry.setBallot(state.getCurrentBallot());
        logEntry.setTwoPcTag(tag);
        logEntry.advancePhase(Phase.ACCEPTED);

        log("2PC PROPOSE tag=%s seq=%d ballot=%s txn %s->%s amt=%d",
                tag.name(), seq, ballotString(state.getCurrentBallot()),
                req.getSender(), req.getReceiver(), req.getAmount());

        AcceptMsg accept = AcceptMsg.newBuilder()
                .setBallot(state.getCurrentBallot().toProto())
                .setSeq(seq)
                .setReq(req)
                .setTwoPcTag(tag)
                .build();
        boolean success = replication.sendAccept(accept);
        if (!success) {


            log("2PC PROPOSE tag=%s seq=%d quorum-failed", tag.name(), seq);
            return null;
        }

        logEntry.advancePhase(Phase.COMMITTED);
        CommitMsg commit = CommitMsg.newBuilder()
                .setBallot(state.getCurrentBallot().toProto())
                .setSeq(seq)
                .setReq(req)
                .setTwoPcTag(tag)
                .build();
        replication.sendCommit(commit);

        try {
            stateMachine.executeStateMachine();
        } catch (Throwable ignored) { }

        log("2PC COMMIT tag=%s broadcast seq=%d", tag.name(), seq);



        stateMachine.executeStateMachine();




        log("2PC RESULT tag=%s seq=%d assumed success after quorum", tag.name(), seq);
        return Boolean.TRUE;
    }

    private Integer findExistingSeqForRequest(ClientRequest req) {
        if (req == null) return null;
        String clientId = req.getClientId();
        long ts = req.getTimestamp();
        if (clientId == null || clientId.isEmpty() || ts <= 0L) {
            return null;
        }
        Integer best = null;
        for (Types.LogEntry le : state.getLog().values()) {
            ClientRequest r = le.getReq();
            if (r == null) continue;
            if (!clientId.equals(r.getClientId())) continue;
            if (r.getTimestamp() != ts) continue;
            if (best == null || le.getSeq() < best) {
                best = le.getSeq();
            }
        }
        return best;
    }


    private void handleCheckpointMetadata(int seq, String digest, int sourceId) {
        if (!checkpointingEnabled || seq <= 0) {
            return;
        }
        int lastExecuted = state.getLastExecutedSeq();
        if (lastExecuted >= seq) {
            state.noteCheckpointFromPeer(seq, digest);
            log("CHECKPOINT ack seq=%d already executed=%d", seq, lastExecuted);
            return;
        }

        Map<String, Integer> snapshot = fetchCheckpointSnapshotFromAny(seq, digest, sourceId);
        if (snapshot == null || snapshot.isEmpty()) {
            log("CHECKPOINT snapshot unavailable or mismatched seq=%d; deferring", seq);
            return;
        }

        state.applyCheckpointSnapshot(snapshot, seq, digest);
        log("CHECKPOINT applied seq=%d entries=%d", seq, snapshot.size());
        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.CHECKPOINT,
                sourceId, null, seq, null, "applied");
        flushStateMachine();
    }

    private void tryToPublishCP() {
        if (!checkpointingEnabled) return;
        if (!state.getIsLeader()) return;
        int seq = state.pollPendingCheckpointSeq();
        if (seq <= 0) return;

        Map<String, Integer> snapshot = state.takeStagedCheckpointIfSeq(seq);
        if (snapshot == null || snapshot.isEmpty()) {
            snapshot = state.getDatabase().snapshot();
        }
        String digest = Types.computeDigest(snapshot);
        state.setLastCheckpointSnapshot(snapshot);
        state.updateCheckpointMetadata(seq, digest);
        CheckpointMsg.Builder builder = CheckpointMsg.newBuilder()
                .setSeq(seq)
                .setSourceNodeId(state.getNodeConfig().getNodeId());
        if (digest != null && !digest.isEmpty()) {
            builder.setDigest(digest);
        }
        CheckpointMsg msg = builder.build();
        log("CHECKPOINT broadcast seq=%d digest=%s period=%d", seq, digest, checkpointPeriod);
        for (var entry : stubs.entrySet()) {
            int peerId = entry.getKey();
            if (!state.isPeerAllowed(peerId)) continue;
            try {
                MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.CHECKPOINT,
                        peerId, null, seq, null, "send");
                entry.getValue().checkpoint(msg);
            } catch (Exception ex) {
                log("CHECKPOINT send failed peer=%d seq=%d err=%s", peerId, seq, ex.getMessage());
            }
        }

        try {
            java.util.LinkedHashSet<Integer> peersToCatch = new java.util.LinkedHashSet<>();
            for (var e : stubs.entrySet()) {
                int pid = e.getKey();
                if (state.isPeerAllowed(pid)) peersToCatch.add(pid);
            }
            if (!peersToCatch.isEmpty()) {
                catchUpNewPeers(peersToCatch);
            }
        } catch (Throwable t) {
            log("CHECKPOINT commit-sweep failed: %s", t.getMessage());
        }
    }

    private NodeServiceGrpc.NodeServiceBlockingStub selectCheckpointSource(int sourceId) {
        int self = state.getNodeConfig().getNodeId();
        if (sourceId == self) return null;
        if (sourceId != 0) {
            var stub = stubs.get(sourceId);
            if (stub != null) return stub;
        }
        int leaderId = state.getKnownLeaderId();
        if (leaderId != 0 && leaderId != self) {
            var stub = stubs.get(leaderId);
            if (stub != null) return stub;
        }
        for (var entry : stubs.entrySet()) {
            if (!state.isPeerAllowed(entry.getKey())) continue;
            return entry.getValue();
        }
        return null;
    }

    private Map<String, Integer> toMap(DBDump dump) {
        Map<String, Integer> map = new java.util.HashMap<>();
        for (KV kv : dump.getBalancesList()) {
            map.put(kv.getKey(), kv.getValue());
        }
        return map;
    }

    private Map<String, Integer> fetchCheckpointSnapshotFromAny(int seq, String digest, int initialSourceId) {
        int self = state.getNodeConfig().getNodeId();
        java.util.LinkedHashSet<Integer> candidates = new java.util.LinkedHashSet<>();
        if (initialSourceId != 0 && initialSourceId != self) candidates.add(initialSourceId);
        int leaderId = state.getKnownLeaderId();
        if (leaderId != 0 && leaderId != self) candidates.add(leaderId);
        for (int peerId : stubs.keySet()) {
            if (!state.isPeerAllowed(peerId)) continue;
            if (peerId != self) candidates.add(peerId);
        }

        if (initialSourceId == self || leaderId == self) {
            Map<String,Integer> snap = state.getLastCheckpointSnapshot();
            if (snap != null && !snap.isEmpty()) {
                if (digest == null || digest.isEmpty() || Types.computeDigest(snap).equals(digest)) {
                    return snap;
                }
            }
        }

        for (int pid : candidates) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = stubs.get(pid);
            if (stub == null) continue;
            try {
                DBDump dump = stub
                        .withDeadlineAfter(Math.max(FOLLOWER_FORWARD_TIMEOUT_MS, 500L), java.util.concurrent.TimeUnit.MILLISECONDS)
                        .getCheckpointSnapshot(Empty.getDefaultInstance());
                Map<String,Integer> snap = toMap(dump);
                if (snap == null || snap.isEmpty()) continue;
                if (digest == null || digest.isEmpty() || Types.computeDigest(snap).equals(digest)) {
                    return snap;
                }
            } catch (Exception ex) {
            }
        }
        return null;
    }


    private void flushStateMachine() {
        stateMachine.executeStateMachine();
        tryToPublishCP();
    }

    private void cacheReplyIfFinal(ClientState cs, Reply r) {
        if (!isFinalReply(r)) {
            return;
        }
        synchronized (cs) {
            cs.recordReply(r.getTimestamp(), r);
        }
    }

    private void resetLocalPrepareState(String txnId, TwoPcState txState, java.util.List<String> lockedKeys) {
        if (lockedKeys != null && !lockedKeys.isEmpty()) {
            state.unlockAccounts(lockedKeys);
        }
        if (txState != null) {
            txState.setLockedKeys(null);
        }
        if (txnId != null) {
            Types.TwoPcWalEntry wal = state.removeWal(txnId);
            if (wal != null) {
                state.getDatabase().setBalance(wal.getAccountId(), wal.getOldBalance());
            }
            TwoPcState removed = state.getTwoPcStates().remove(txnId);
            if (removed != null && removed.getLogSeq() > 0) {
                state.getLog().remove(removed.getLogSeq());
            }
        }
    }

    private static boolean isFinalReply(Reply r) {
        return r != null && (r.getSuccess() || r.getResultType().equals(Reply.ResultType.FAILURE));
    }
}
