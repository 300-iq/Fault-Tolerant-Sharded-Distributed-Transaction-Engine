package paxos.node;

import paxos.common.MessageAudit;
import paxos.common.Types;
import paxos.common.Types.*;
import paxos.proto.NodeServiceGrpc;
import paxos.proto.PaxosProto;
import paxos.proto.PaxosProto.*;
import paxos.utils.PrintHelper;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class ElectionCoordinator {

    private final NodeState state;
    private final Replication replication;
    private final StateMachine stateMachine;
    private final Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs;
    private final ExecutorService pool;
    private final int quorum;
    private final String logPrefix;

    private volatile long lastLeaderMsgTime = System.nanoTime();
    private volatile long lastPrepareSentTime = 0L;
    private volatile long lastPrepareReceivedTime = 0L;

    private final long heartbeat;
    private final long minLeaderTimeout, maxLeaderTimeout;
    private final long minPrepareTimeout, maxPrepareTimeout;
    private final long replicationTimeout;
    private final long tpMs;

    private final SplittableRandom random;
    private final ScheduledExecutorService scheduler;

    private final Object prepareLock = new Object();
    private volatile BallotNum highestDeferredPrepare = null;
    private volatile boolean recovering = false;

    private volatile long currentElectionTimeout;
    private volatile long currentPrepareTimeout;

    private volatile BallotNum highestSeenBallot;

    private final List<PaxosProto.NewViewMsg> viewHistory = new CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicBoolean electionRunning = new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile BallotNum acceptNextPrepareAtLeast = null;

    private static final class CheckpointContext {
        final int seq;
        final String digest;
        final int sourceId;

        CheckpointContext(int seq, String digest, int sourceId) {
            this.seq = seq;
            this.digest = (digest == null) ? "" : digest;
            this.sourceId = sourceId;
        }
    }


    public ElectionCoordinator(NodeState state,
                        Replication replication,
                        StateMachine stateMachine,
                        Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs,
                        ExecutorService pool,
                        int quorum,
                        long heartbeat,
                        long minLeaderTimeout, long maxLeaderTimeout,
                        long minPrepareTimeout, long maxPrepareTimeout,
                        long replicationTimeout,
                        long prepareThrottle) {
        this.state = state;
        this.replication = replication;
        this.stateMachine = stateMachine;
        this.stubs = stubs;
        this.pool = pool;
        this.quorum = quorum;
        this.logPrefix = "[Node " + state.getNodeConfig().getNodeId() + "] ";

        this.heartbeat = heartbeat;
        this.minLeaderTimeout = minLeaderTimeout;
        this.maxLeaderTimeout = maxLeaderTimeout;
        this.minPrepareTimeout = minPrepareTimeout;
        this.maxPrepareTimeout = maxPrepareTimeout;
        this.replicationTimeout = replicationTimeout;
        this.tpMs = prepareThrottle;

        this.random = new SplittableRandom(Objects.hash(state.getNodeConfig().getNodeId()));
        this.currentElectionTimeout = jitter(minLeaderTimeout, maxLeaderTimeout);
        this.currentPrepareTimeout = jitter(minPrepareTimeout, maxPrepareTimeout);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "paxos-timers-" + state.getNodeConfig().getNodeId());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thr, ex) ->
                    System.err.println("Timer thread died on node " + state.getNodeConfig().getNodeId() + ": " + ex));
            return t;
        });

        this.highestSeenBallot = state.getCurrentBallot();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::tick, 200, 200, MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::sendHeartbeatIfLeader, 0, heartbeat, MILLISECONDS);
        log("timers started heartbeat=%dms electionWindow=[%d,%d]ms", heartbeat, minLeaderTimeout, maxLeaderTimeout);
    }
    public void shutdown() { scheduler.shutdownNow(); }

    private void log(String fmt, Object... args) {
        PrintHelper.debugf(logPrefix + fmt, args);
    }

    public void gotLeaderMessage() {
        lastLeaderMsgTime = now();
        
        currentPrepareTimeout = jitter(minPrepareTimeout, maxPrepareTimeout);
        int leaderId = state.getKnownLeaderId();
        BallotNum cur = state.getCurrentBallot();
        BallotNum hi = state.getHighestSeenBallot();

    }

    public void gotPrepareMessage() {
        lastPrepareReceivedTime = now();
        log("prepare seen recently, throttling new election attempts");
    }

    public void noteBallotSeen(BallotNum b) {
        if (highestSeenBallot == null || b.ge(highestSeenBallot)) highestSeenBallot = b;
        if (state.getHighestSeenBallot() == null || b.ge(state.getHighestSeenBallot())) {
            state.setHighestSeenBallot(b);
        }

    }


    public boolean timedOut() { return (now() - lastLeaderMsgTime) >= msToNanos(currentElectionTimeout); }

    public List<NewViewMsg> viewHistory() { return viewHistory; }
    public void clearViewHistory() { viewHistory.clear(); }

    public void resetViewHistory() { viewHistory.clear(); }
    public boolean isRecovering() { return recovering; }

    private static long now() { return System.nanoTime(); }
    private static long msToNanos(long ms) { return ms * 1_000_000L; }

    private long jitter(long minMsIncl, long maxMsIncl) {
        long span = Math.max(0L, maxMsIncl - minMsIncl);
        return minMsIncl + (span == 0 ? 0 : random.nextLong(span + 1));
    }

    private synchronized boolean canPrepare() {
        return (now() - lastPrepareReceivedTime) >= msToNanos(tpMs);
    }

    private void tick() {
        if (!state.isSelfAllowed()) return;
        if (state.isTimersPaused() || state.isLfSuspended()) return;
        if (state.getIsLeader()) return;

        long sinceLeaderMsg   = now() - lastLeaderMsgTime;
        long sincePrepMsgSent = now() - lastPrepareSentTime;

        boolean noMsgFromLeader    = sinceLeaderMsg   >= msToNanos(currentElectionTimeout);
        boolean prepareBackOffDone = sincePrepMsgSent >= msToNanos(currentPrepareTimeout);

        if (noMsgFromLeader) {

            BallotNum deferred = takeDeferredIfTimedOut();
            if (deferred != null && state.promiseBallot(deferred)) {
                highestSeenBallot = deferred;
                state.noteHigherBallot(deferred);
                acceptNextPrepareAtLeast = deferred;

                lastLeaderMsgTime = now();
                currentPrepareTimeout = jitter(minPrepareTimeout, maxPrepareTimeout);
                log("timer expired: promised deferred PREPARE %s", deferred);
                return;
            }
            if (peekDeferredPrepare() != null) {

                currentPrepareTimeout = jitter(minPrepareTimeout, maxPrepareTimeout);
                return;
            }
        }

        if (!(noMsgFromLeader && prepareBackOffDone && canPrepare())) return;

        if (!electionRunning.compareAndSet(false, true)) return;

        final long nextPrepareTimeout = jitter(minPrepareTimeout, maxPrepareTimeout);
        lastPrepareSentTime = now();

        pool.submit(() -> {
            try {
                boolean won = startElection();
                if (won) {
                    currentElectionTimeout = jitter(minLeaderTimeout, maxLeaderTimeout);
                    log("became leader with ballot %s", state.getCurrentBallot());
                } else {
                    log("election attempt failed; backing off to %dms", nextPrepareTimeout);
                }
            } finally {
                currentPrepareTimeout = nextPrepareTimeout;
                electionRunning.set(false);
            }
        });
    }


    private void sendHeartbeatIfLeader() {
        if (!state.getIsLeader()) return;
        if (state.isTimersPaused() || state.isLfSuspended()) return;
        var cur = state.getCurrentBallot();
        var hi  = state.getHighestSeenBallot();
        if (cur != null && hi != null && hi.compareTo(cur) > 0) {
            state.setLeader(false);
            return;
        }
        if (!state.isSelfAllowed()) {
            state.setLeader(false);
            return;
        }

        Ballot current = state.getCurrentBallot() != null ? state.getCurrentBallot().toProto() : Ballot.getDefaultInstance();
        HeartbeatMsg hb = HeartbeatMsg.newBuilder()
                .setNodeId(state.getNodeConfig().getNodeId())
                .setBallot(current)
                .build();
        stubs.forEach((peerId, stub) -> {
            if (!state.isPeerAllowed(peerId)) return;
            pool.submit(() -> {
                try {
                    stub.heartbeat(hb);
                    //log("heartbeat -> peer=%d leader=%d ballot=%s sent",
                            //peerId, state.getNodeConfig().getNodeId(), state.getCurrentBallot());
                } catch (Exception ex) {
                    log("heartbeat -> peer failed: %s", ex.getMessage());
                }
            });
        });

        gotLeaderMessage();


    }

    private synchronized boolean startElection() {
        if (!state.isSelfAllowed()) {
            log("election skipped: node filtered out");
            return false;
        }

        BallotNum b = state.allocateNextBallot(highestSeenBallot);

        highestSeenBallot = b;
        state.setHighestSeenBallot(b);
        log("starting election with ballot %s", b);
        PrepareMsg prepareMsg = PrepareMsg.newBuilder().setBallot(b.toProto()).build();
        List<PromiseMsg> promises = Collections.synchronizedList(new ArrayList<>());
        promises.add(buildLocalPromise(b));

        CountDownLatch done = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean higherSeen = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicReference<BallotNum> hintedBallot = new java.util.concurrent.atomic.AtomicReference<>();
        stubs.forEach((peerId, stub) -> {
            if (!state.isPeerAllowed(peerId)) {
                log("prepare skipped peer=%d (marked offline)", peerId);
                return;
            }
            pool.submit(() -> {
                try {

                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.PREPARE,
                            peerId, prepareMsg.getBallot(), null, null, null);
                    PromiseMsg promiseMsg = stub.prepare(prepareMsg);
                    BallotNum replyBallot = BallotNum.of(promiseMsg.getBallot());
                    if (promiseMsg.getOk() && replyBallot.equals(b)) {

                        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.PROMISE,
                                peerId, promiseMsg.getBallot(), null, null, "ok accCount=" + promiseMsg.getAccCount());
                        promises.add(promiseMsg);
                        log("promise from peer ballot=%s acc=%d", replyBallot, promiseMsg.getAccCount());
                        if (promises.size() >= quorum) done.countDown();
                    } else {

                        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.PROMISE,
                                peerId, promiseMsg.getBallot(), null, null, "nack reason=" + promiseMsg.getReason());
                        log("promise nack from peer reason=%s", promiseMsg.getReason());


                        boolean higherThanProposed = (replyBallot != null && replyBallot.compareTo(b) > 0);
                        if (higherThanProposed) {
                            hintedBallot.updateAndGet(prev -> {
                                if (prev == null || replyBallot.ge(prev)) {
                                    return replyBallot;
                                }
                                return prev;
                            });
                            higherSeen.set(true);
                            done.countDown();
                        }

                    }
                } catch (Exception ex) {
                    log("prepare RPC failed: %s", ex.getMessage());
                }
            });
        });

        try { done.await(maxLeaderTimeout, MILLISECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }

        if (higherSeen.get()) {
            BallotNum hinted = hintedBallot.get();
            if (hinted != null) {
                highestSeenBallot = hinted;
                state.noteHigherBallot(hinted);
                state.learnLeader(hinted, hinted.getNodeId());
            }
            currentPrepareTimeout = Math.min(maxPrepareTimeout,
                    Math.max(currentPrepareTimeout, minPrepareTimeout) * 2);
            state.setLeader(false);
            return false;
        }

        if (promises.size() < quorum) {
            currentPrepareTimeout = Math.min(maxPrepareTimeout,
                    Math.max(currentPrepareTimeout, minPrepareTimeout) * 2);
            return false;
        }

        log("quorum formed with %d promises", promises.size());

        CheckpointContext checkpoint = summarizeCheckpoint(promises);
        if (checkpoint.seq > 0) {

            if (state.getLastExecutedSeq() < checkpoint.seq) {
                Map<String, Integer> snapshot = fetchCheckpointSnapshot(checkpoint);
                if (snapshot != null && !snapshot.isEmpty()) {
                    state.applyCheckpointSnapshot(snapshot, checkpoint.seq, checkpoint.digest);
                    stateMachine.executeStateMachine();
                    log("checkpoint applied during election seq=%d source=%d", checkpoint.seq, checkpoint.sourceId);
                } else {
                    log("checkpoint unavailable during election seq=%d source=%d; proceeding with cp in NEW-VIEW; followers may fetch", checkpoint.seq, checkpoint.sourceId);
                }
            }

            if (state.getLastExecutedSeq() >= checkpoint.seq) {
                state.noteCheckpointFromPeer(checkpoint.seq, checkpoint.digest);
            }
        }

        recovering = true;

        NewViewMsg newViewMsg = createNewView(b, promises, checkpoint);
        int maxSeq = 0;
        for (AcceptMsg a : newViewMsg.getAcceptsList()) {
            maxSeq = Math.max(maxSeq, a.getSeq());
        }
        maxSeq = Math.max(maxSeq, newViewMsg.getCheckpointSeq());
        if (maxSeq > 0) {
            state.advanceNextSeqIfNeeded(maxSeq);
        }

        state.setCurrentBallot(b);
        state.learnLeader(b, state.getNodeConfig().getNodeId());
        state.setKnownLeaderId(state.getNodeConfig().getNodeId());
        state.setLeader(true);

        viewHistory.add(newViewMsg);
        log("broadcasting NEW-VIEW(with acks) with %d accepts", newViewMsg.getAcceptsCount());

        boolean ok = broadcastNewViewWithAcks(newViewMsg);
        if (!ok) {
            log("new-view acks did not reach quorum for all entries; abdicating leadership.");
            state.setLeader(false);
            recovering = false;
            return false;
        }
        log("new-view acks reached quorum and commits broadcasted.");

        recovering = false;
        return true;

    }


    private NewViewMsg createNewView(BallotNum b, List<PromiseMsg> promises, CheckpointContext checkpoint) {

        Map<Integer, AcceptMsg> pick = new TreeMap<>();
        int maxSeq = 0;

        for (PromiseMsg promiseMsg : promises) {
            for (AcceptMsg acceptMsg : promiseMsg.getAccList()) {
                maxSeq = Math.max(maxSeq, acceptMsg.getSeq());
                AcceptMsg cur = pick.get(acceptMsg.getSeq());
                if (cur == null || BallotNum.of(acceptMsg.getBallot()).ge(BallotNum.of(cur.getBallot()))) {
                    pick.put(acceptMsg.getSeq(), acceptMsg);
                }
            }
        }

        int checkpointSeq = checkpoint == null ? 0 : Math.max(0, checkpoint.seq);
        NewViewMsg.Builder nv = NewViewMsg.newBuilder().setBallot(b.toProto());
        if (checkpointSeq > 0) {
            nv.setCheckpointSeq(checkpointSeq);
            if (checkpoint != null && !checkpoint.digest.isEmpty()) {
                nv.setCheckpointDigest(checkpoint.digest);
            }
        }
        int startSeq = Math.max(1, checkpointSeq + 1);
        for (int sNo = startSeq; sNo <= maxSeq; sNo++) {
            ClientRequest req;
            AcceptMsg chosen = pick.get(sNo);
            TwoPcTag tag;
            if (chosen == null) {

                req = Types.NOOP;
                tag = TwoPcTag.NONE;
            } else {
                req = chosen.getReq();
                tag = chosen.getTwoPcTag();
            }
            nv.addAccepts(AcceptMsg.newBuilder()
                    .setBallot(b.toProto())
                    .setSeq(sNo)
                    .setReq(req)
                    .setTwoPcTag(tag)
                    .build());
        }
        return nv.build();
    }

    private CheckpointContext summarizeCheckpoint(List<PromiseMsg> promises) {
        int seq = 0;
        String digest = "";
        int sourceId = 0;
        int self = state.getNodeConfig().getNodeId();
        for (PromiseMsg pm : promises) {
            int candidate = pm.getCheckpointSeq();
            if (candidate <= 0) {
                continue;
            }
            if (candidate > seq) {
                seq = candidate;
                digest = pm.getCheckpointDigest();
                sourceId = pm.getNodeId();
            } else if (candidate == seq) {
                if (sourceId != self && pm.getNodeId() == self) {
                    sourceId = self;
                }
                if ((digest == null || digest.isEmpty()) && !pm.getCheckpointDigest().isEmpty()) {
                    digest = pm.getCheckpointDigest();
                }
            }
        }
        if (seq == 0) {
            return new CheckpointContext(0, "", 0);
        }
        if (sourceId == 0) {
            sourceId = self;
        }
        if (digest == null) {
            digest = "";
        }
        return new CheckpointContext(seq, digest, sourceId);
    }

    private Map<String, Integer> fetchCheckpointSnapshot(CheckpointContext checkpoint) {
        if (checkpoint == null || checkpoint.seq <= 0) {
            return null;
        }
        int self = state.getNodeConfig().getNodeId();
        int sourceId = checkpoint.sourceId == 0 ? self : checkpoint.sourceId;
        if (sourceId == self) {
            Map<String,Integer> snap = state.getLastCheckpointSnapshot();
            if (snap != null && !snap.isEmpty()) return snap;
            return null;
        }
        NodeServiceGrpc.NodeServiceBlockingStub stub = stubs.get(sourceId);
        if (stub == null) {
            log("checkpoint source stub missing for node=%d", sourceId);
            return null;
        }
        try {
            DBDump dump = stub.withDeadlineAfter(Math.max(replicationTimeout, 800L), MILLISECONDS)
                    .getCheckpointSnapshot(Empty.getDefaultInstance());
            Map<String, Integer> map = new HashMap<>();
            for (KV kv : dump.getBalancesList()) {
                map.put(kv.getKey(), kv.getValue());
            }

            if (checkpoint.digest != null && !checkpoint.digest.isEmpty()) {
                String got = paxos.common.Types.computeDigest(map);
                if (!checkpoint.digest.equals(got)) {
                    return null;
                }
            }
            return map;
        } catch (Exception ex) {
            log("checkpoint fetch failed source=%d seq=%d err=%s", sourceId, checkpoint.seq, ex.getMessage());
            return null;
        }
    }

    private PromiseMsg buildLocalPromise(BallotNum ballot) {
        PromiseMsg.Builder builder = PromiseMsg.newBuilder()
                .setBallot(ballot.toProto())
                .setOk(true)
                .setCheckpointSeq(state.getLastCheckpointSeq())
                .setCheckpointDigest(state.getLastCheckpointDigest())
                .setNodeId(state.getNodeConfig().getNodeId());
        state.getLog().values().stream()
                .sorted(Comparator.comparingInt(LogEntry::getSeq))
                .forEach(le -> {
                    if (le.getPhase().ordinal() >= Phase.ACCEPTED.ordinal()) {
                        ClientRequest r = le.getReq();
                        if (r == null) {
                            r = Types.NOOP;
                        }
                        builder.addAcc(AcceptMsg.newBuilder()
                                .setBallot(le.getBallot().toProto())
                                .setSeq(le.getSeq())
                                .setReq(r)
                                .setTwoPcTag(le.getTwoPcTag())
                                .build());
                    }
                });
        return builder.build();
    }

    private boolean broadcastNewViewWithAcks(NewViewMsg newViewMsg) {

        Set<Integer> seqs = new java.util.HashSet<>();
        Map<Integer, java.util.concurrent.atomic.AtomicInteger> ackCounts = new java.util.concurrent.ConcurrentHashMap<>();
        Set<Integer> committedSeqs = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

        BallotNum targetBallot = BallotNum.of(newViewMsg.getBallot());
        for (AcceptMsg a : newViewMsg.getAcceptsList()) {
            seqs.add(a.getSeq());
            ackCounts.put(a.getSeq(), new java.util.concurrent.atomic.AtomicInteger(1));
            Types.LogEntry local = state.getLog().computeIfAbsent(a.getSeq(), s -> new Types.LogEntry(s, a.getReq(), targetBallot));
            if (local.getPhase() != Phase.EXECUTED) {
                local.setReq(a.getReq());
                local.setBallot(targetBallot);
                local.setTwoPcTag(a.getTwoPcTag());
                local.advancePhase(Phase.ACCEPTED);
            }
        }

        int possibleAcks = 1 + (int) stubs.keySet().stream().filter(state::isPeerAllowed).count();
        if (possibleAcks < quorum) {
            log("NEW-VIEW acks cannot reach quorum: possible=%d < quorum=%d", possibleAcks, quorum);
            return false;
        }

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch((int) stubs.keySet().stream().filter(state::isPeerAllowed).count());
        java.util.concurrent.atomic.AtomicBoolean lostLeadership = new java.util.concurrent.atomic.AtomicBoolean(false);

        stubs.forEach((peerId, stub) -> {
            if (!state.isPeerAllowed(peerId)) return;
            pool.submit(() -> {
                try {

                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.NEW_VIEW,
                            peerId, newViewMsg.getBallot(), null, null, "accepts=" + newViewMsg.getAcceptsCount());
                    NewViewReply reply = stub.newViewWithAcks(newViewMsg);

                    for (Accepted ack : reply.getAcksList()) {

                        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.ACCEPTED,
                                peerId, ack.getB(), ack.getSeq(), null, "new-view");
                        if (lostLeadership.get()) break;

                        BallotNum ackBallot = ack.hasB() ? BallotNum.of(ack.getB()) : null;
                        if (!ack.getOk()) {
                            if (ackBallot != null && ackBallot.compareTo(targetBallot) > 0) {
                                lostLeadership.set(true);
                                break;
                            }
                            continue;
                        }
                        if (ackBallot == null || !ackBallot.equals(targetBallot)) {
                            continue;
                        }
                        int seq = ack.getSeq();
                        if (!seqs.contains(seq)) continue;
                        int count = ackCounts.get(seq).incrementAndGet();
                        if (count >= quorum && committedSeqs.add(seq)) {

                            Types.LogEntry local = state.getLog().computeIfAbsent(seq, s -> new Types.LogEntry(s, null, targetBallot));
                            if (local.getPhase() != Phase.EXECUTED) {
                                local.setBallot(targetBallot);
                                local.advancePhase(Phase.COMMITTED);
                            }
                            CommitMsg commit = CommitMsg.newBuilder()
                                    .setBallot(newViewMsg.getBallot()).setSeq(seq)
                                    .setReq(local.getReq() == null ? Types.NOOP : local.getReq())
                                    .setTwoPcTag(local.getTwoPcTag())
                                    .build();
                            replication.sendCommit(commit);
                            log("NEW-VIEW commit broadcast seq=%d", seq);
                        }
                    }
                } catch (Exception ex) {
                    log("NEW-VIEW with acks failed toward peer=%d: %s", peerId, ex.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        });

        try {
            boolean finished = latch.await(replicationTimeout, MILLISECONDS);
            if (!finished) {
                log("NEW-VIEW acks: timeout waiting for follower replies");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        if (lostLeadership.get()) {
            log("NEW-VIEW acks: higher ballot observed → abdicate");
            return false;
        }

        for (int seq : seqs) {
            if (ackCounts.get(seq).get() >= quorum && committedSeqs.add(seq)) {
                Types.LogEntry local = state.getLog().computeIfAbsent(seq, s -> new Types.LogEntry(s, null, targetBallot));
                if (local.getPhase() != Phase.EXECUTED) {
                    local.setBallot(targetBallot);
                    local.advancePhase(Phase.COMMITTED);
                }
                CommitMsg commit = CommitMsg.newBuilder()
                        .setBallot(newViewMsg.getBallot()).setSeq(seq)
                        .setReq(local.getReq() == null ? Types.NOOP : Types.isNoop(local.getReq()) ? Types.NOOP : local.getReq())
                        .setTwoPcTag(local.getTwoPcTag())
                        .build();
                replication.sendCommit(commit);
                log("NEW-VIEW commit broadcast seq=%d", seq);
            }
        }

        java.util.Set<Integer> lackingSeqs = new java.util.LinkedHashSet<>();
        for (int seq : seqs) {
            if (ackCounts.get(seq).get() < quorum && !committedSeqs.contains(seq)) {
                lackingSeqs.add(seq);
            }
        }

        if (!lackingSeqs.isEmpty()) {
            log("NEW-VIEW acks missing quorum for seqs=%s; retrying via ACCEPT fan-out", lackingSeqs);
            for (AcceptMsg a : newViewMsg.getAcceptsList()) {
                if (!lackingSeqs.contains(a.getSeq())) continue;
                if (!state.getIsLeader()) {
                    log("NEW-VIEW recovery aborted: lost leadership during retry");
                    return false;
                }
                BallotNum acceptBallot = BallotNum.of(a.getBallot());
                if (!targetBallot.equals(acceptBallot)) {
                    log("NEW-VIEW recovery aborted: ballot mismatch for seq=%d", a.getSeq());
                    return false;
                }
                boolean accepted = replication.sendAccept(a, replicationTimeout);
                if (!accepted) {
                    log("NEW-VIEW recovery ACCEPT seq=%d failed; aborting election", a.getSeq());
                    return false;
                }
                ackCounts.get(a.getSeq()).set(quorum);
                if (committedSeqs.add(a.getSeq())) {
                    Types.LogEntry local = state.getLog().computeIfAbsent(a.getSeq(), s -> new Types.LogEntry(s, a.getReq(), targetBallot));
                    if (local.getPhase() != Phase.EXECUTED) {
                        local.setReq(a.getReq());
                        local.setBallot(targetBallot);
                        local.setTwoPcTag(a.getTwoPcTag());
                        local.advancePhase(Phase.COMMITTED);
                    }
                    CommitMsg commit = CommitMsg.newBuilder()
                            .setBallot(newViewMsg.getBallot()).setSeq(a.getSeq())
                            .setReq(local.getReq() == null ? Types.NOOP : Types.isNoop(local.getReq()) ? Types.NOOP : local.getReq())
                            .setTwoPcTag(local.getTwoPcTag())
                            .build();
                    replication.sendCommit(commit);
                    log("NEW-VIEW recovery commit broadcast seq=%d", a.getSeq());
                }
            }
        }

        for (int seq : seqs) {
            if (ackCounts.get(seq).get() < quorum && !committedSeqs.contains(seq)) {
                log("NEW-VIEW acks: quorum still missing for seq=%d; aborting", seq);
                return false;
            }
        }

        stateMachine.executeStateMachine();
        return true;
    }
    public void recordDeferredPrepare(BallotNum ballot) {
        synchronized (prepareLock) {
            if (highestDeferredPrepare == null || ballot.ge(highestDeferredPrepare)) {
                highestDeferredPrepare = ballot;
            }
        }
        log("deferred PREPARE ballot=%s", ballot);
    }

    public boolean allowPrepareBallot(BallotNum ballot) {
        synchronized (prepareLock) {

            if (!timedOut()) {
                return false;
            }
            BallotNum deferred = highestDeferredPrepare;
            if (deferred != null && !ballot.ge(deferred)) {
                return false;
            }
            if (deferred != null && ballot.ge(deferred)) {
                highestDeferredPrepare = null;
            }
            return true;
        }
    }

    public synchronized BallotNum peekDeferredPrepare() {
        return highestDeferredPrepare;
    }

    public void triggerElectionNow() {
        if (state.getIsLeader()) return;

        lastLeaderMsgTime = 0L;
        pool.submit(this::tick);
    }


    public void probeElectionSoon() {
        if (state.getIsLeader()) return;
        pool.submit(this::tick);
    }

    private BallotNum takeDeferredIfTimedOut() {
        synchronized (prepareLock) {
            if (!timedOut()) return null;
            BallotNum d = highestDeferredPrepare;
            highestDeferredPrepare = null;
            return d;
        }
    }



    public boolean shouldAcceptPrepareNow(BallotNum b) {
        synchronized (prepareLock) {
            if (acceptNextPrepareAtLeast != null && b.ge(acceptNextPrepareAtLeast)) {

                acceptNextPrepareAtLeast = null;
                highestDeferredPrepare = null;
                return true;
            }
            if (!timedOut()) return false;
            if (highestDeferredPrepare != null && !b.ge(highestDeferredPrepare)) return false;
            highestDeferredPrepare = null;
            return true;
        }
    }

}
