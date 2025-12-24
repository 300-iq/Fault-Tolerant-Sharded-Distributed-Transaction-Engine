package paxos.node;

import paxos.common.Types;
import paxos.common.Types.*;
import paxos.proto.PaxosProto.*;
import paxos.utils.PrintHelper;
import paxos.common.MessageAudit;

public class StateMachine {

    private final Object execLock = new Object();
    private final NodeState state;

    public StateMachine(NodeState state) {
        this.state = state;
    }

    public void executeStateMachine() {
        synchronized (execLock) {
            for (;;) {
                final int seq = state.getNextExecSeq();
                final LogEntry le = state.getLog().get(seq);
                if (le == null || le.getPhase().ordinal() < Phase.COMMITTED.ordinal()) return;
                if (le.getPhase() == Phase.EXECUTED) {
                    state.bumpExecSeq();
                    continue;
                }

                boolean success = true;
                boolean done = true;
                try {
                    final ClientRequest req = le.getReq();
                    final boolean isNoop = paxos.common.Types.isNoop(req);

                    if (!isNoop) {
                        TwoPcTag tag = le.getTwoPcTag();
                        if (tag == TwoPcTag.NONE) {
                            final String clientId = req.getClientId();
                            boolean applyDb = true;
                            Types.ClientState cs = null;
                            if (!clientId.isEmpty()) {
                                cs = state.getClientStates().computeIfAbsent(clientId, k -> new Types.ClientState());
                                synchronized (cs) {
                                    if (cs.hasExecutedTs(req.getTimestamp())) {
                                        applyDb = false;
                                        Reply prev = cs.getReplyForTs(req.getTimestamp());
                                        success = (prev != null) ? prev.getSuccess() : true;
                                    }
                                }
                            }

                            if (applyDb) {
                                success = state.getDatabase().transfer(req.getSender(), req.getReceiver(), req.getAmount());
                                if (success) {
                                    state.recordModifiedKeys(req.getSender(), req.getReceiver());
                                }
                                try {
                                    MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.EXECUTE,
                                            null, le.getBallot().toProto(), seq, req, "apply ok=" + success);
                                } catch (Throwable ignored) { }
                            } else {
                                try {
                                    MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.EXECUTE,
                                            null, le.getBallot().toProto(), seq, req, "skip duplicate ts" );
                                } catch (Throwable ignored) { }
                            }
                            if (!clientId.isEmpty() && cs != null) {
                                synchronized (cs) {
                                    Reply dummy = Reply.newBuilder()
                                            .setSuccess(success)
                                            .setMessage(success ? "OK" : "Insufficient funds")
                                            .setTimestamp(req.getTimestamp()).setResultType(success ? Reply.ResultType.SUCCESS : Reply.ResultType.FAILURE)
                                            .setLeaderBallot(le.getBallot().toProto())
                                            .build();
                                    cs.recordReply(req.getTimestamp(), dummy);
                                }
                            }

                            PrintHelper.debugf(
                                    "Node %d EXEC s=%d %s %s->%s amt=%d ok=%s",
                                    state.getNodeConfig().getNodeId(), seq,
                                    req.getClientId(), req.getSender(), req.getReceiver(), req.getAmount(), success
                            );
                        } else {
                            boolean[] doneArr = new boolean[1];
                            success = handleTwoPcEntry(seq, le, tag, doneArr);
                            done = doneArr[0];
                        }
                    } else {
                        //System.out.printf("Node %d EXEC s=%d NO-OP%n", state.getNodeConfig().getNodeId(), seq);
                    }
                } catch (Throwable t) {
                    success = false;
                    try {
                        PrintHelper.debugf("Node %d EXEC exception seq=%d tag=%s err=%s",
                                state.getNodeConfig().getNodeId(), seq, le.getTwoPcTag(), t.toString());
                    } catch (Throwable ignored) { }
                } finally {
                    TwoPcTag tag = le.getTwoPcTag();
                    if (tag == TwoPcTag.NONE) {
                        le.advancePhase(Phase.EXECUTED);
                        le.getExecResult().complete(success);
                        state.bumpExecSeq();
                        state.recordExecution(seq);
                        state.unlockBySeq(seq);
                    } else {
                        if (tag == TwoPcTag.PREPARE) {
                            le.getTwoPcPrepareResult().complete(success);
                            le.advancePhase(Phase.EXECUTED);
                            state.bumpExecSeq();
                            state.recordExecution(seq);
                            state.unlockBySeq(seq);
                        } else if (tag == TwoPcTag.COMMIT || tag == TwoPcTag.ABORT) {
                            le.getTwoPcFinalResult().complete(success);
                            if (done) {
                                le.advancePhase(Phase.EXECUTED);
                                state.bumpExecSeq();
                                state.recordExecution(seq);
                                state.unlockBySeq(seq);
                            }
                        }
                    }
                    if (tag != TwoPcTag.NONE) {



                        return;
                    }
                }
            }
        }
    }

    private boolean handleTwoPcEntry(int seq, LogEntry le, TwoPcTag tag, boolean[] doneOut) {
        ClientRequest req = le.getReq();
        if (doneOut != null && doneOut.length > 0) {
            doneOut[0] = false;
        }
        if (req == null || paxos.common.Types.isNoop(req)) {
            return true;
        }
        String txnId = Types.txnId(req);
        if (txnId == null) {
            return false;
        }
        Types.DB db = state.getDatabase();
        Types.TwoPcState txState = state.getTwoPcStates().computeIfAbsent(txnId,
                id -> new Types.TwoPcState(id, req));
        switch (tag) {
            case PREPARE:
                return handleTwoPcPrepareEntry(txnId, txState, req, db, le, seq);
            case COMMIT:
                if (txState.hasFinalDecision() && !txState.isCommitDecision()) {
                    if (doneOut != null && doneOut.length > 0) doneOut[0] = true;
                    return true; // ignore conflicting abort after commit already decided
                }
                if (txState.hasFinalDecision() && txState.isCommitDecision() && txState.isFinalApplied()) {
                    if (doneOut != null && doneOut.length > 0) doneOut[0] = true;
                    return true;
                }
                if (handleTwoPcCommitEntry(txnId, txState, db, le, seq)) {
                    if (doneOut != null && doneOut.length > 0) doneOut[0] = true;
                    return true;
                }
                return false;
            case ABORT:
                if (txState.hasFinalDecision() && txState.isCommitDecision()) {
                    if (doneOut != null && doneOut.length > 0) doneOut[0] = true;
                    return true; // ignore abort after commit already decided
                }
                if (txState.hasFinalDecision() && !txState.isCommitDecision() && txState.isFinalApplied()) {
                    if (doneOut != null && doneOut.length > 0) doneOut[0] = true;
                    return true;
                }
                if (handleTwoPcAbortEntry(txnId, txState, db, le, seq)) {
                    if (doneOut != null && doneOut.length > 0) doneOut[0] = true;
                    return true;
                }
                return false;
            default:
                return true;
        }
    }

    private static final int DEFAULT_BALANCE = 10;

    private int clusterForAccountByIdRange(String acctId) {
        if (acctId == null) return 0;
        try {
            int id = Integer.parseInt(acctId.trim());
            if (id >= 1 && id <= 3000) return 1;
            if (id >= 3001 && id <= 6000) return 2;
            if (id >= 6001 && id <= 9000) return 3;
        } catch (NumberFormatException ignored) {}
        return 0;
    }

    private int clusterForNode(int nodeId) {
        if (nodeId >= 1 && nodeId <= 3) return 1;
        if (nodeId >= 4 && nodeId <= 6) return 2;
        if (nodeId >= 7 && nodeId <= 9) return 3;
        return 0;
    }

    /**
     * Determines if an account is local to this cluster.
     * Uses NodeState.isAccountOwned() which checks the allAccounts list.
     * This list is properly updated by reshard operations.
     */
    private boolean isAccountLocal(String acctId) {
        if (acctId == null) return false;
        return state.isAccountOwned(acctId);
    }

    private boolean handleTwoPcPrepareEntry(String txnId,
                                            Types.TwoPcState txState,
                                            ClientRequest req,
                                            Types.DB db,
                                            LogEntry le,
                                            int seq) {
        if (txState.isLocalPrepared() ||
                txState.getPhase().ordinal() >= Types.TwoPcPhase.PREPARED.ordinal()) {
            return true;
        }
        String sender = req.getSender();
        String receiver = req.getReceiver();
        int amount = req.getAmount();
        
        // Determine sender/receiver side using allAccounts list (properly tracks reshard ownership)
        boolean senderLocal = isAccountLocal(sender);
        boolean receiverLocal = isAccountLocal(receiver);
        
        // Get balances, defaulting to initial balance if account doesn't exist
        Integer senderBalRaw = (sender == null ? null : db.getBalance(sender));
        Integer receiverBalRaw = (receiver == null ? null : db.getBalance(receiver));
        int senderBal = (senderBalRaw != null) ? senderBalRaw : DEFAULT_BALANCE;
        int receiverBal = (receiverBalRaw != null) ? receiverBalRaw : DEFAULT_BALANCE;
        
        if (senderLocal && !receiverLocal) {
            if (sender == null || amount <= 0) {
                return false;
            }
            int oldBal = senderBal;
            if (oldBal < amount) {
                return false;
            }
            int newBal;
            try {
                newBal = Math.subtractExact(oldBal, amount);
            } catch (ArithmeticException e) {
                return false;
            }
            Types.TwoPcWalEntry wal = new Types.TwoPcWalEntry(sender, oldBal, newBal);
            Types.TwoPcWalEntry existing = state.putWalIfAbsent(txnId, wal);
            if (existing != null) {
                wal = existing;
                newBal = wal.getNewBalance();
            }
            try {
                PrintHelper.debugf("Node %d PREPARE(S) seq=%d txn=%s sender=%s old=%d new=%d reusedWal=%s",
                        state.getNodeConfig().getNodeId(), seq, txnId, sender, oldBal, newBal,
                        existing != null ? "yes" : "no");
            } catch (Throwable ignored) { }
            // DON'T apply balance during PREPARE - COMMIT will apply delta for deterministic replay
            // This also correctly handles overlapping transactions on the same account
            synchronized (txState) {
                txState.setRole(Types.TwoPcRole.COORDINATOR);
                txState.markLocalPrepared();
            }
            try {
                MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.EXECUTE,
                        null, le.getBallot().toProto(), seq, req,
                        "2PC_PREPARE_SENDER");
            } catch (Throwable ignored) { }
            return true;
        }
        if (!senderLocal && receiverLocal) {
            if (receiver == null || amount <= 0) {
                return false;
            }
            int oldBal = receiverBal;
            int newBal;
            try {
                newBal = Math.addExact(oldBal, amount);
            } catch (ArithmeticException e) {
                return false;
            }
            Types.TwoPcWalEntry wal = new Types.TwoPcWalEntry(receiver, oldBal, newBal);
            Types.TwoPcWalEntry existing = state.putWalIfAbsent(txnId, wal);
            if (existing != null) {
                wal = existing;
                newBal = wal.getNewBalance();
            }
            try {
                PrintHelper.debugf("Node %d PREPARE(R) seq=%d txn=%s recv=%s old=%d new=%d reusedWal=%s",
                        state.getNodeConfig().getNodeId(), seq, txnId, receiver, oldBal, newBal,
                        existing != null ? "yes" : "no");
            } catch (Throwable ignored) { }
            // DON'T apply balance during PREPARE - COMMIT will apply delta for deterministic replay
            // This also correctly handles overlapping transactions on the same account
            synchronized (txState) {
                txState.setRole(Types.TwoPcRole.PARTICIPANT);
                txState.markLocalPrepared();
            }
            try {
                MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.EXECUTE,
                        null, le.getBallot().toProto(), seq, req,
                        "2PC_PREPARE_RECEIVER");
            } catch (Throwable ignored) { }
            return true;
        }
        return true;
    }

    private boolean handleTwoPcCommitEntry(String txnId,
                                           Types.TwoPcState txState,
                                           Types.DB db,
                                           LogEntry le,
                                           int seq) {
        Types.TwoPcWalEntry wal = state.removeWal(txnId);
        ClientRequest req = txState.getRequest();
        if (wal != null) {
            // Apply delta (newBalance - oldBalance) to current balance for deterministic replay
            // This correctly handles overlapping transactions on the same account
            int delta = wal.getNewBalance() - wal.getOldBalance();
            Integer curBal = db.getBalance(wal.getAccountId());
            int currentBalance = (curBal != null) ? curBal : 10; // default initial balance
            int finalBalance = currentBalance + delta;
            try {
                PrintHelper.debugf("Node %d COMMIT seq=%d txn=%s WAL apply delta acct=%s cur=%d delta=%d final=%d",
                        state.getNodeConfig().getNodeId(), seq, txnId, wal.getAccountId(),
                        currentBalance, delta, finalBalance);
            } catch (Throwable ignored) { }
            db.setBalance(wal.getAccountId(), finalBalance);
            state.recordModifiedKeys(wal.getAccountId(), wal.getAccountId());
        } else if (req != null) {


            String sender = req.getSender();
            String receiver = req.getReceiver();
            int amount = req.getAmount();
            Integer senderBal = (sender == null ? null : db.getBalance(sender));
            Integer receiverBal = (receiver == null ? null : db.getBalance(receiver));
            boolean hasSender = senderBal != null;
            boolean hasReceiver = receiverBal != null;
            try {
                PrintHelper.debugf("Node %d COMMIT seq=%d txn=%s WAL missing -> direct apply s=%s bal=%s r=%s bal=%s amt=%d",
                        state.getNodeConfig().getNodeId(), seq, txnId,
                        sender, (senderBal == null ? "null" : senderBal.toString()),
                        receiver, (receiverBal == null ? "null" : receiverBal.toString()),
                        amount);
            } catch (Throwable ignored) { }
            if (amount > 0 && hasSender ^ hasReceiver) {
                try {
                    if (hasSender && !hasReceiver) {
                        int oldBal = senderBal;
                        if (oldBal >= amount) {
                            int newBal = Math.subtractExact(oldBal, amount);
                            db.setBalance(sender, newBal);
                            state.recordModifiedKeys(sender, sender);
                        }
                    } else if (!hasSender && hasReceiver) {
                        int newBal = Math.addExact(receiverBal, amount);
                        db.setBalance(receiver, newBal);
                        state.recordModifiedKeys(receiver, receiver);
                    }
                } catch (ArithmeticException ignored) {

                }
            }
        }

        synchronized (txState) {
            if (!txState.hasFinalDecision()) {
                txState.decideCommit(true);
            }
            txState.markFinalApplied();
        }
        if (req != null) {
            String sender = req.getSender();
            Integer bal = (sender == null ? null : db.getBalance(sender));
            boolean coordinatorSide = sender != null && bal != null;
            if (coordinatorSide) {
                String clientId = req.getClientId();
                if (clientId != null && !clientId.isEmpty()) {
                    Types.ClientState cs = state.getClientStates().computeIfAbsent(clientId,
                            k -> new Types.ClientState());
                    synchronized (cs) {
                        Reply dummy = Reply.newBuilder()
                                .setSuccess(true)
                                .setMessage("OK")
                                .setTimestamp(req.getTimestamp())
                                .setResultType(Reply.ResultType.SUCCESS)
                                .setLeaderBallot(le.getBallot().toProto())
                                .build();
                        cs.recordReply(req.getTimestamp(), dummy);
                    }
                }
            }
        }
        try {
            MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.EXECUTE,
                    null, le.getBallot().toProto(), seq, req, "2PC_COMMIT");
        } catch (Throwable ignored) { }
        return true;
    }

    private boolean handleTwoPcAbortEntry(String txnId,
                                          Types.TwoPcState txState,
                                          Types.DB db,
                                          LogEntry le,
                                          int seq) {
        Types.TwoPcWalEntry wal = state.removeWal(txnId);
        // PREPARE no longer changes balance, so ABORT just removes WAL - no balance restoration needed
        // Do NOT touch modified set - another committed transaction may have modified this account

        synchronized (txState) {
            if (!txState.hasFinalDecision()) {
                txState.decideCommit(false);
            }
            txState.markFinalApplied();
        }
        ClientRequest req = txState.getRequest();
        if (req != null) {
            String sender = req.getSender();
            Integer bal = (sender == null ? null : db.getBalance(sender));
            boolean coordinatorSide = sender != null && bal != null;
            if (coordinatorSide) {
                String clientId = req.getClientId();
                if (clientId != null && !clientId.isEmpty()) {
                    Types.ClientState cs = state.getClientStates().computeIfAbsent(clientId,
                            k -> new Types.ClientState());
                    synchronized (cs) {
                        Reply dummy = Reply.newBuilder()
                                .setSuccess(false)
                                .setMessage("Transaction aborted")
                                .setTimestamp(req.getTimestamp())
                                .setResultType(Reply.ResultType.FAILURE)
                                .setLeaderBallot(le.getBallot().toProto())
                                .build();
                        cs.recordReply(req.getTimestamp(), dummy);
                    }
                }
            }
        }
        try {
            MessageAudit.log(state, MessageAudit.Dir.RECV, MessageAudit.Kind.EXECUTE,
                    null, le.getBallot().toProto(), seq, req, "2PC_ABORT");
        } catch (Throwable ignored) { }
        return true;
    }
}
