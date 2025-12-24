package paxos.common;


import paxos.proto.PaxosProto.*;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

// I was intially many hitting race conditions here with multiple threads involved, I took help of chatGPT to make my code more robust and thread safe.

public class Types {

    public static class BallotNum {
        private final int round;
        private final int nodeId;
        public BallotNum(int round, int nodeId){ this.round=round; this.nodeId=nodeId; }
        public boolean ge(BallotNum other){
            if (other == null) return true;
            if (this.round != other.round) return this.round >= other.round; 
            return this.nodeId >= other.nodeId;
        }

        public int getRound() {
            return round;
        }

        public int getNodeId() {
            return nodeId;
        }

        public Ballot toProto(){ return Ballot.newBuilder().setRound(round).setNodeId(nodeId).build(); }
        public static BallotNum of(Ballot b){ return new BallotNum(b.getRound(), b.getNodeId()); }
        @Override public String toString(){ return "("+round+","+nodeId+")"; }

        @Override public boolean equals(Object o){
            if (this == o) return true;
            if (!(o instanceof BallotNum)) return false;
            BallotNum other = (BallotNum) o;
            return this.round == other.round && this.nodeId == other.nodeId;
        }
        @Override public int hashCode(){ return java.util.Objects.hash(round, nodeId); }
        public int compareTo(BallotNum other){
            if (this.round != other.round) return Integer.compare(this.round, other.round);
            return Integer.compare(this.nodeId, other.nodeId);
        }
    }

    public enum Phase { NONE, ACCEPTED, COMMITTED, EXECUTED }

    public enum TwoPcRole { NONE, COORDINATOR, PARTICIPANT }

    public enum TwoPcPhase {
        NONE,
        PREPARING,
        PREPARED,
        COMMITTING,
        ABORTING,
        COMMITTED,
        ABORTED
    }

    public static String txnId(String clientId, long ts) {
        if (clientId == null) return null;
        if (ts <= 0L) return null;
        String trimmed = clientId.trim();
        if (trimmed.isEmpty()) return null;
        return trimmed + "#" + ts;
    }

    public static String txnId(ClientRequest req) {
        if (req == null) return null;
        return txnId(req.getClientId(), req.getTimestamp());
    }


    public static final ClientRequest NOOP = ClientRequest.newBuilder()
            .setClientId("")
            .setTimestamp(0)
            .setSender("")
            .setReceiver("")
            .setAmount(0)
            .build();

    public static boolean isNoop(ClientRequest req) {
        return req == null || (req.getClientId().isEmpty() && req.getTimestamp() == 0 && req.getAmount() == 0);
    }

    // I took help of chatGPT to understand and generate digests
    public static String computeDigest(Map<String, Integer> balances) {
        if (balances == null || balances.isEmpty()) {
            return "";
        }
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
        TreeMap<String, Integer> ordered = new TreeMap<>(balances);
        StringBuilder sb = new StringBuilder(ordered.size() * 16);
        for (Map.Entry<String, Integer> entry : ordered.entrySet()) {
            sb.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        byte[] hash = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
        return toHex(hash);
    }

    private static String toHex(byte[] bytes) {
        char[] hexArray = "0123456789abcdef".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }


    public static class LogEntry {
        private final int seq;
        private volatile ClientRequest req;
        private volatile BallotNum ballot;
        private volatile Phase phase = Phase.NONE;
        private final java.util.concurrent.CompletableFuture<Boolean> execResult = new java.util.concurrent.CompletableFuture<>();
        private final java.util.concurrent.CompletableFuture<Boolean> twoPcPrepareResult = new java.util.concurrent.CompletableFuture<>();
        private final java.util.concurrent.CompletableFuture<Boolean> twoPcFinalResult = new java.util.concurrent.CompletableFuture<>();
        private final Object phaseLock = new Object();
        private volatile TwoPcTag twoPcTag = TwoPcTag.NONE;

        public LogEntry(int seq, ClientRequest req, BallotNum ballot){
            this.seq=seq; this.req=req; this.ballot=ballot;
        }

        public int getSeq() {
            return seq;
        }

        public ClientRequest getReq() {
            return req;
        }

        public void setReq(ClientRequest req) {
            this.req = req;
        }

        public BallotNum getBallot() {
            return ballot;
        }

        public void setBallot(BallotNum ballot) {
            this.ballot = ballot;
        }

        public Phase getPhase() {
            synchronized (phaseLock) {
                return phase;
            }
        }



        public CompletableFuture<Boolean> getExecResult() {
            return execResult;
        }

        public CompletableFuture<Boolean> getTwoPcPrepareResult() {
            return twoPcPrepareResult;
        }

        public CompletableFuture<Boolean> getTwoPcFinalResult() {
            return twoPcFinalResult;
        }

        public void advancePhase(Phase target) {
            synchronized (phaseLock) {
                if (this.phase.ordinal() < target.ordinal()) {
                    this.phase = target;
                }
            }
        }

        public TwoPcTag getTwoPcTag() {
            return twoPcTag;
        }

        public void setTwoPcTag(TwoPcTag tag) {
            if (tag == null) {
                tag = TwoPcTag.NONE;
            }
            this.twoPcTag = tag;
        }
    }


    public static class ClientState {
        private long lastTs = -1;
        private Reply lastReply = null;
        private long pendingTs = -1;
        private final LinkedHashMap<Long, Reply> history = new LinkedHashMap<Long, Reply>(128, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Reply> eldest) {
                return this.size() > 1024;
            }
        };

        public synchronized long getPendingTs() { return pendingTs; }
        public synchronized void setPendingTs(long ts) { pendingTs = ts; }
        public synchronized void clearPending() { pendingTs = -1; }

        public synchronized void recordReply(long ts, Reply r) {
            if (ts >= lastTs) {
                lastTs = ts;
                lastReply = r;
            }
            if (r != null) {
                history.put(ts, r);
            }
            if (pendingTs == ts) {
                pendingTs = -1;
            }
        }


        public synchronized long getLastTs() { return lastTs; }

        public synchronized Reply getLastReply() { return lastReply; }

        public synchronized boolean hasExecutedTs(long ts) { return history.containsKey(ts); }
        public synchronized Reply getReplyForTs(long ts) { return history.get(ts); }
    }


    public static final class TwoPcWalEntry {
        private final String accountId;
        private final int oldBalance;
        private final int newBalance;

        public TwoPcWalEntry(String accountId, int oldBalance, int newBalance) {
            this.accountId = accountId;
            this.oldBalance = oldBalance;
            this.newBalance = newBalance;
        }

        public String getAccountId() { return accountId; }
        public int getOldBalance() { return oldBalance; }
        public int getNewBalance() { return newBalance; }
    }


    public static class TwoPcState {
        private final String txnId;
        private final ClientRequest request;
        private TwoPcRole role;
        private TwoPcPhase phase;
        private boolean localPrepared;
        private boolean finalDecisionKnown;
        private boolean finalDecisionCommit;
        private boolean finalDecisionApplied;
        private java.util.List<String> lockedKeys;
        private int logSeq;
        private int finalSeq;
        private long createdNs;
        private long lastUpdateNs;
        private TwoPcPhase lastObservedPhase;
        private int participantRetryCount;

        public TwoPcState(String txnId, ClientRequest request) {
            this.txnId = txnId;
            this.request = request;
            this.role = TwoPcRole.NONE;
            this.phase = TwoPcPhase.NONE;
            this.logSeq = 0;
            this.finalSeq = 0;
            this.participantRetryCount = 0;
            long now = System.nanoTime();
            this.createdNs = now;
            this.lastUpdateNs = now;
            this.lastObservedPhase = TwoPcPhase.NONE;
        }

        public synchronized String getTxnId() { return txnId; }

        public synchronized ClientRequest getRequest() { return request; }

        public synchronized TwoPcRole getRole() { return role; }

        public synchronized void setRole(TwoPcRole role) {
            if (role == null) return;
            this.role = role;
        }

        public synchronized TwoPcPhase getPhase() { return phase; }

        public synchronized void setPhase(TwoPcPhase next) {
            if (next == null) return;
            if (this.phase.ordinal() <= next.ordinal()) {
                if (this.phase != next) {
                    this.phase = next;
                    long now = System.nanoTime();
                    this.lastUpdateNs = now;
                    this.lastObservedPhase = next;
                }
            }
        }
        public synchronized void touch() {
            this.lastUpdateNs = System.nanoTime();
        }

        public synchronized boolean isLocalPrepared() { return localPrepared; }

        public synchronized void markLocalPrepared() {
            localPrepared = true;
            if (phase.ordinal() < TwoPcPhase.PREPARED.ordinal()) {
                phase = TwoPcPhase.PREPARED;
            }
            long now = System.nanoTime();
            this.lastUpdateNs = now;
            this.lastObservedPhase = this.phase;
        }

        public synchronized boolean hasFinalDecision() { return finalDecisionKnown; }

        public synchronized boolean isCommitDecision() { return finalDecisionCommit; }

        public synchronized boolean isFinalApplied() { return finalDecisionApplied; }

        public synchronized void decideCommit(boolean commit) {
            finalDecisionKnown = true;
            finalDecisionCommit = commit;
            phase = commit ? TwoPcPhase.COMMITTED : TwoPcPhase.ABORTED;
            long now = System.nanoTime();
            this.lastUpdateNs = now;
            this.lastObservedPhase = this.phase;
        }

        public synchronized void markFinalApplied() {
            finalDecisionApplied = true;
        }

        public synchronized java.util.List<String> getLockedKeys() {
            if (lockedKeys == null || lockedKeys.isEmpty()) {
                return java.util.Collections.emptyList();
            }
            return java.util.Collections.unmodifiableList(new java.util.ArrayList<>(lockedKeys));
        }

        public synchronized void setLockedKeys(java.util.List<String> keys) {
            if (keys == null || keys.isEmpty()) {
                this.lockedKeys = null;
            } else {
                this.lockedKeys = new java.util.ArrayList<>(keys);
            }
        }

        public synchronized boolean hasLogSeq() {
            return logSeq > 0;
        }

        public synchronized int getLogSeq() {
            return logSeq;
        }

        public synchronized void setLogSeqIfUnset(int seq) {
            if (seq > 0 && logSeq == 0) {
                logSeq = seq;
                lastUpdateNs = System.nanoTime();
            }
        }

        public synchronized boolean hasFinalSeq() {
            return finalSeq > 0;
        }

        public synchronized int getFinalSeq() {
            return finalSeq;
        }

        public synchronized void setFinalSeqIfUnset(int seq) {
            if (seq > 0 && finalSeq == 0) {
                finalSeq = seq;
                lastUpdateNs = System.nanoTime();
            }
        }

        public synchronized long getCreatedNs() {
            return createdNs;
        }

        public synchronized long getLastUpdateNs() {
            return lastUpdateNs;
        }

        public synchronized void touchForTimer() {
            long now = System.nanoTime();
            this.lastUpdateNs = now;
            this.lastObservedPhase = this.phase;
        }

        public synchronized TwoPcPhase getLastObservedPhase() {
            return lastObservedPhase;
        }

        public synchronized int getParticipantRetryCount() {
            return participantRetryCount;
        }

        public synchronized int incrementParticipantRetryCount() {
            return ++participantRetryCount;
        }
    }


    // Used chatGpt for help with the persisting DB to CSV files part
    public static class DB {

        private final Path dataPath;
        private final org.mapdb.DB mapDb;
        private final org.mapdb.HTreeMap<String, Integer> database;
        private final boolean commitEachTxn;

        public DB(Collection<String> accounts, int initial) {
            this(accounts, initial, null);
        }

        public DB(Collection<String> accounts, int initial, Path dataPath) {
            this.dataPath = dataPath;

            boolean reset = Boolean.parseBoolean(System.getProperty("resetDb", "true"));

            org.mapdb.DB db;
            org.mapdb.HTreeMap<String, Integer> map;

            if (dataPath != null) {
                java.io.File file = dataPath.toFile();
                java.io.File parent = file.getParentFile();
                if (parent != null && !parent.exists()) {
                    parent.mkdirs();
                }
                db = org.mapdb.DBMaker
                        .fileDB(file)
                        .fileMmapEnableIfSupported()
                        .transactionEnable()
                        .closeOnJvmShutdown()
                        .make();
                map = db.hashMap("balances", org.mapdb.Serializer.STRING, org.mapdb.Serializer.INTEGER)
                        .createOrOpen();
            } else {
                db = org.mapdb.DBMaker
                        .memoryDB()
                        .transactionEnable()
                        .closeOnJvmShutdown()
                        .make();
                map = db.hashMap("balances", org.mapdb.Serializer.STRING, org.mapdb.Serializer.INTEGER)
                        .createOrOpen();
            }

            this.mapDb = db;
            this.database = map;

            boolean performanceMode = Boolean.parseBoolean(System.getProperty("performanceMode", "false"));
            this.commitEachTxn = Boolean.parseBoolean(
                    System.getProperty("commitEachTxn", performanceMode ? "false" : "true"));

            synchronized (this) {
                if (reset) {
                    initFresh(accounts, initial);
                    safeCommit();
                } else if (database.isEmpty()) {
                    initFresh(accounts, initial);
                    safeCommit();
                } else {
                    for (String a : accounts) {
                        database.putIfAbsent(a, initial);
                    }
                    safeCommit();
                }
            }
        }

        private void initFresh(Collection<String> accounts, int initial) {
            database.clear();
            for (String a : accounts) {
                database.put(a, initial);
            }
        }

        private void safeCommit() {
            try {
                mapDb.commit();
            } catch (Exception ex) {
                if (ex instanceof IOException io) {
                    warnPersistFailure(io);
                } else {
                    warnPersistFailure(new IOException(ex));
                }
            }
        }

        public synchronized boolean transfer(String s, String r, int amt) {
            if (s == null || r == null || s.isEmpty() || r.isEmpty()) return false;
            if (s.equals(r)) return true;
            if (amt <= 0) return false;
            if (!database.containsKey(s) || !database.containsKey(r)) return false;

            int sb = database.get(s);
            if (sb < amt) return false;

            database.put(s, sb - amt);
            Integer rv = database.get(r);
            if (rv == null) {
                database.put(r, amt);
            } else {
                database.put(r, rv + amt);
            }

            if (commitEachTxn) {
                safeCommit();
            }
            return true;
        }

        public synchronized Integer getBalance(String accountId) {
            if (accountId == null) return null;
            return database.get(accountId);
        }

        public synchronized boolean setBalance(String accountId, int newBalance) {
            if (accountId == null) {
                return false;
            }


            database.put(accountId, newBalance);
            if (commitEachTxn) {
                safeCommit();
            }
            return true;
        }

        public synchronized Map<String, Integer> snapshot() {
            return new TreeMap<>(database);
        }

        public synchronized void replaceWith(Map<String, Integer> balances) {
            database.clear();
            if (balances != null) {
                database.putAll(balances);
            }
            safeCommit();
        }

        public Map<String, Integer> diskSnapshot() {
            synchronized (this) {
                return new TreeMap<>(database);
            }
        }

        public void close() {
            try {
                mapDb.close();
            } catch (Exception ignored) {
            }
        }

        private void warnPersistFailure(IOException e) {
            try {
                System.err.println("[DB] Persist failure to " + (dataPath == null ? "<memory>" : dataPath.toString()) + ": " + e.getMessage());
            } catch (Throwable ignored) { }
        }
    }

}
