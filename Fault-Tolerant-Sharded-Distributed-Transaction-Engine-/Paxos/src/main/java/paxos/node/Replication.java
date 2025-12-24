package paxos.node;

import paxos.common.MessageAudit;
import paxos.common.Types;
import paxos.proto.NodeServiceGrpc;
import paxos.proto.PaxosProto.*;
import paxos.utils.PrintHelper;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class Replication {
    private final Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs;
    private final ExecutorService pool;
    private final int quorum;
    private final NodeState state;


    private final long defaultAcceptTimeoutMs;

    public Replication(NodeState state,
                       Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs,
                       int quorum,
                       ExecutorService pool) {
        this(state, stubs, quorum, pool, Long.getLong("replTimeoutMs", 800L));
    }

    public Replication(NodeState state,
                       Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs,
                       int quorum,
                       ExecutorService pool,
                       long defaultAcceptTimeoutMs) {
        this.state = state;
        this.stubs = stubs;
        this.quorum = quorum;
        this.pool = pool;
        this.defaultAcceptTimeoutMs = defaultAcceptTimeoutMs;
    }

    private void log(String fmt, Object... args) {
        PrintHelper.debugf("[Repl] " + fmt, args);
    }


    public boolean sendAccept(AcceptMsg accept) {
        return sendAccept(accept, defaultAcceptTimeoutMs);
    }

    public boolean sendAccept(AcceptMsg accept, long timeoutMs) {
        if (state.isLfSuspended()) {
            log("ACCEPT blocked: leader suspended");
            return false;
        }
        final AtomicInteger acks = new AtomicInteger(1);
        final CountDownLatch done = new CountDownLatch(1);

        final AtomicInteger outcome = new AtomicInteger(0);
        final AtomicReference<Types.BallotNum> hintedBallot = new AtomicReference<>();

        log("ACCEPT fanout seq=%d ballot=(%d,%d) targets=%d",
                accept.getSeq(), accept.getBallot().getRound(), accept.getBallot().getNodeId(), stubs.size());

        int possibleAcks = 1 + (int) stubs.keySet().stream().filter(state::isPeerAllowed).count();
        if (possibleAcks < quorum) {
            log("ACCEPT cannot reach quorum: possible=%d < quorum=%d", possibleAcks, quorum);
            return false;
        }


        final Types.BallotNum leaderBallot = Types.BallotNum.of(accept.getBallot());
        final AtomicInteger remainingPossible = new AtomicInteger(possibleAcks - 1);

        for (var entry : stubs.entrySet()) {
            final int peerId = entry.getKey();
            final var stub = entry.getValue();
            if (!state.isPeerAllowed(peerId)) {
                log("ACCEPT skip peer=%d (marked offline)", peerId);
                continue;
            }
            pool.submit(() -> {

                if (outcome.get() != 0) return;
                try {

                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.ACCEPT,
                            peerId, accept.getBallot(), accept.getSeq(), accept.getReq(), null);
                    Accepted ack = stub.accept(accept);
                    if (ack.getOk()) {

                        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.ACCEPTED,
                                peerId, ack.getB(), ack.getSeq(), null, "ok");
                        int count = acks.incrementAndGet();
                        log("ACCEPT ack from peer=%d count=%d", peerId, count);
                        if (count >= quorum && outcome.compareAndSet(0, 1)) {
                            done.countDown();
                            return;
                        }

                        int rem = remainingPossible.decrementAndGet();
                        if (outcome.get() == 0 && (acks.get() + rem) < quorum) {
                            if (outcome.compareAndSet(0, -1)) {
                                done.countDown();
                            }
                        }
                    } else {

                        MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.ACCEPTED,
                                peerId, ack.getB(), ack.getSeq(), null, "nack reason=" + ack.getReason());
                        log("ACCEPT nack from peer=%d reason=%s", peerId, ack.getReason());
                        if (ack.hasB()) {
                            Types.BallotNum hint = Types.BallotNum.of(ack.getB());
                            hintedBallot.updateAndGet(prev -> (prev == null || hint.compareTo(prev) >= 0) ? hint : prev);
                            if (hint.compareTo(leaderBallot) > 0) {
                                if (outcome.compareAndSet(0, -1)) {
                                    done.countDown();
                                }
                                return;
                            }
                        }
                        int rem = remainingPossible.decrementAndGet();
                        if (outcome.get() == 0 && (acks.get() + rem) < quorum) {
                            if (outcome.compareAndSet(0, -1)) {
                                done.countDown();
                            }
                        }
                    }
                } catch (Exception ex) {
                    log("ACCEPT rpc failure peer=%d err=%s", peerId, ex.getMessage());
                    int rem = remainingPossible.decrementAndGet();
                    if (outcome.get() == 0 && (acks.get() + rem) < quorum) {
                        if (outcome.compareAndSet(0, -1)) {
                            done.countDown();
                        }
                    }
                }
            });
        }

        try {
            boolean signaled = done.await(timeoutMs, TimeUnit.MILLISECONDS);
            int res = outcome.get();
            if (res == 1) {
                log("ACCEPT quorum satisfied seq=%d", accept.getSeq());
                return true;
            }
            if (res == -1) {
                Types.BallotNum hint = hintedBallot.get();
                if (hint != null) {
                    state.learnLeader(hint, hint.getNodeId());
                }
                state.setLeader(false);
                return false;
            }
            log("ACCEPT quorum timeout seq=%d (acks=%d/%d)", accept.getSeq(), acks.get(), quorum);
            return false;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }



    public void sendCommit(CommitMsg commit) {
        if (state.isLfSuspended()) {
            log("COMMIT blocked: leader suspended");
            return;
        }
        log("COMMIT fanout seq=%d ballot=(%d,%d) targets=%d", commit.getSeq(), commit.getBallot().getRound(), commit.getBallot().getNodeId(), stubs.size());
        for (var entry : stubs.entrySet()) {
            final int peerId = entry.getKey();
            final var stub = entry.getValue();
            if (!state.isPeerAllowed(peerId)) {
                log("COMMIT skip peer=%d (marked offline)", peerId);
                continue;
            }
            pool.submit(() -> {
                try {
                    MessageAudit.log(state, MessageAudit.Dir.SENT, MessageAudit.Kind.COMMIT,
                            peerId, commit.getBallot(), commit.getSeq(), commit.getReq(), null);
                    stub.commit(commit);
                    log("COMMIT sent to peer=%d", peerId);
                } catch (Exception ex) {
                    log("COMMIT failed to peer=%d err=%s", peerId, ex.getMessage());
                }
            });
        }
    }
}
