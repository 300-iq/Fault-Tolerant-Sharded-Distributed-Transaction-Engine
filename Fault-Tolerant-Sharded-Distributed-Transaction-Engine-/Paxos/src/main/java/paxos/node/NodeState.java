package paxos.node;

import paxos.common.MessageAudit;
import paxos.common.Types.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CancellationException;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public final class NodeState {
    private final NodeConfig nodeConfig;
    private final java.util.List<String> allAccounts;
    private final int initialBalance;
    private final ConcurrentMap<Integer, LogEntry> log = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ClientState> clientStates = new ConcurrentHashMap<>();
    private final DB database;
    private final AtomicInteger nextSeq = new AtomicInteger(1);
    private final ConcurrentSkipListSet<Integer> recycledSeqs = new ConcurrentSkipListSet<>();
    private final AtomicInteger nextExecSeq = new AtomicInteger(1);
    private final ConcurrentMap<String, AtomicBoolean> lockTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, java.util.List<String>> locksBySeq = new ConcurrentHashMap<>();
    private final Set<String> modifiedAccounts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private final ConcurrentMap<String, TwoPcState> twoPcPhases = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TwoPcWalEntry> twoPcWriteAheadLog;
    private final org.mapdb.DB walDb;
    private final HTreeMap<String, String> walStore;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private volatile BallotNum currentBallot;
    private volatile BallotNum highestSeenBallot;

    private final boolean checkpointingEnabled;
    private final int checkpointPeriod;
    private final AtomicInteger lastCheckpointSeq = new AtomicInteger(0);
    private final java.util.concurrent.atomic.AtomicReference<String> lastCheckpointDigest = new java.util.concurrent.atomic.AtomicReference<>("");
    private volatile java.util.Map<String, Integer> lastCheckpointSnapshot = null;

    private final AtomicInteger stagedCheckpointSeq = new AtomicInteger(0);
    private volatile java.util.Map<String, Integer> stagedCheckpointSnapshot = null;
    private final AtomicInteger lastExecutedSeq = new AtomicInteger(0);
    private final AtomicInteger pendingCheckpointSeq = new AtomicInteger(0);
    private final Object checkpointLock = new Object();

    private volatile int knownLeaderId = 0;
    private volatile Set<Integer> livePeerIds = Collections.emptySet();
    private volatile boolean selfAllowed = true;

    private final Object ballotLock = new Object();
    private volatile boolean suspendedForSet = false;

    private volatile boolean lfSuspended = false;
    private volatile boolean timersPaused = false;
    private final MessageAudit audit;


    public NodeState(NodeConfig cfg, Collection<String> accounts, int initBal) {
        this.nodeConfig = cfg;
        this.allAccounts = new ArrayList<>(accounts);
        this.initialBalance = initBal;
        boolean performanceMode = Boolean.parseBoolean(System.getProperty("performanceMode", "false"));
        Path dbPath;
        String explicit = System.getProperty("dbPath");
        if (explicit != null && !explicit.isBlank()) {
            dbPath = Paths.get(explicit);
        } else {
            String dataDir = System.getProperty("dataDir", "data");
            dbPath = Paths.get(dataDir, "node-" + cfg.getNodeId() + "-db.db");
        }

        boolean persistDBOnDisk = Boolean.parseBoolean(
                System.getProperty("persistDBOnDisk", performanceMode ? "false" : "true"));
        if (!persistDBOnDisk) {
            dbPath = null;
        }

        this.database = new DB(accounts, initBal, dbPath);
        boolean persistWal = Boolean.parseBoolean(System.getProperty("persistWal", "true"));
        boolean walOnDisk = persistWal;
        org.mapdb.DB wdb = null;
        HTreeMap<String, String> wstore = null;
        this.twoPcWriteAheadLog = new ConcurrentHashMap<>();
        if (walOnDisk) {
            Path walPath = Paths.get(System.getProperty("dataDir", "data"),
                    "node-" + cfg.getNodeId() + "-wal.db");
            wdb = org.mapdb.DBMaker
                    .fileDB(walPath.toFile())
                    .fileMmapEnableIfSupported()
                    .transactionEnable()
                    .closeOnJvmShutdown()
                    .make();
            wstore = wdb.hashMap("wal", Serializer.STRING, Serializer.STRING)
                    .createOrOpen();

            if (!wstore.isEmpty()) {
                for (Map.Entry<String, String> e : wstore.entrySet()) {
                    TwoPcWalEntry entry = decodeWal(e.getValue());
                    if (entry != null) {
                        this.twoPcWriteAheadLog.put(e.getKey(), entry);
                    }
                }
            }
        }
        this.walDb = wdb;
        this.walStore = wstore;
        this.currentBallot = new BallotNum(1, cfg.getLeaderId());
        this.highestSeenBallot = currentBallot;
        isLeader.set(false);
        this.checkpointingEnabled = Boolean.parseBoolean(System.getProperty("enableCheckpointing", "false"));
        int defaultPeriod = 5;
        if (System.getProperty("checkpointPeriod") != null) {
            try {
                defaultPeriod = Integer.parseInt(System.getProperty("checkpointPeriod"));
            } catch (NumberFormatException ignored) { }
        }
        this.checkpointPeriod = Math.max(0, defaultPeriod);
        this.knownLeaderId = cfg.getLeaderId();
        int maxAudit = 50_000;
        try {
            String prop = System.getProperty("auditMaxEntries", performanceMode ? "0" : "50000");
            maxAudit = Integer.parseInt(prop);
        } catch (Exception ignored) { }
        this.audit = new MessageAudit(cfg.getNodeId(), maxAudit);
    }


    public NodeConfig getNodeConfig() { return nodeConfig; }
    public boolean isCheckpointingEnabled() { return checkpointingEnabled; }
    public int getCheckpointPeriod() { return checkpointPeriod; }
    public ConcurrentMap<Integer, LogEntry> getLog() { return log; }
    public ConcurrentMap<String, ClientState> getClientStates() { return clientStates; }
    public DB getDatabase() { return database; }
    public ConcurrentMap<String, TwoPcState> getTwoPcStates() { return twoPcPhases; }
    public ConcurrentMap<String, TwoPcWalEntry> getTwoPcWriteAheadLog() { return twoPcWriteAheadLog; }

    public TwoPcWalEntry putWalIfAbsent(String txnId, TwoPcWalEntry wal) {
        if (txnId == null || wal == null) return null;
        TwoPcWalEntry existing = twoPcWriteAheadLog.putIfAbsent(txnId, wal);
        if (existing == null) {
            persistWal(txnId, wal);
            return wal;
        }
        return existing;
    }

    public TwoPcWalEntry removeWal(String txnId) {
        if (txnId == null) return null;
        TwoPcWalEntry removed = twoPcWriteAheadLog.remove(txnId);
        if (removed != null) {
            try {
                if (walStore != null) {
                    walStore.remove(txnId);
                    walDb.commit();
                }
            } catch (Exception ignored) { }
        }
        return removed;
    }

    private void persistWal(String txnId, TwoPcWalEntry wal) {
        if (walStore == null || txnId == null || wal == null) return;
        try {
            walStore.put(txnId, encodeWal(wal));
            walDb.commit();
        } catch (Exception ignored) { }
    }

    private static String encodeWal(TwoPcWalEntry wal) {
        if (wal == null) return "";
        return wal.getAccountId() + "|" + wal.getOldBalance() + "|" + wal.getNewBalance();
    }

    private static TwoPcWalEntry decodeWal(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        String[] parts = raw.split("\\|");
        if (parts.length != 3) return null;
        try {
            String acct = parts[0];
            int oldB = Integer.parseInt(parts[1]);
            int newB = Integer.parseInt(parts[2]);
            return new TwoPcWalEntry(acct, oldB, newB);
        } catch (Exception e) {
            return null;
        }
    }

    public synchronized void resetForNewSet() {



        BallotNum prevCurrent = currentBallot;
        BallotNum prevHighest = highestSeenBallot;
        int prevKnownLeader = knownLeaderId;
        boolean prevIsLeader = isLeader.get();


        java.util.Map<String,Integer> fresh = new java.util.HashMap<>();
        for (String a : allAccounts) {
            fresh.put(a, initialBalance);
        }
        database.replaceWith(fresh);


        log.clear();
        clientStates.clear();


        nextSeq.set(1);
        recycledSeqs.clear();
        nextExecSeq.set(1);
        lastExecutedSeq.set(0);


        lastCheckpointSeq.set(0);
        lastCheckpointDigest.set("");
        lastCheckpointSnapshot = null;
        stagedCheckpointSeq.set(0);
        stagedCheckpointSnapshot = null;
        pendingCheckpointSeq.set(0);

        currentBallot = prevCurrent != null ? prevCurrent : new BallotNum(1, nodeConfig.getLeaderId());
        highestSeenBallot = prevHighest != null ? prevHighest : currentBallot;
        knownLeaderId = prevKnownLeader;
        isLeader.set(prevIsLeader && selfAllowed);


        lockTable.clear();
        locksBySeq.clear();
        modifiedAccounts.clear();
        twoPcPhases.clear();
        twoPcWriteAheadLog.clear();
        if (walStore != null) {
            try {
                walStore.clear();
                walDb.commit();
            } catch (Exception ignored) { }
        }
    }

    public java.util.Set<String> getModifiedKeysSnapshot() {
        return new java.util.LinkedHashSet<>(modifiedAccounts);
    }

    public void recordModifiedKeys(String s, String r) {
        if (s != null) {
            String t = s.trim();
            if (!t.isEmpty()) {
                modifiedAccounts.add(t);
            }
        }
        if (r != null) {
            String t = r.trim();
            if (!t.isEmpty()) {
                modifiedAccounts.add(t);
            }
        }
    }

    public void removeModifiedKey(String key) {
        if (key == null) return;
        String t = key.trim();
        if (t.isEmpty()) return;
        modifiedAccounts.remove(t);
    }

    public void clearModifiedKeys() {
        modifiedAccounts.clear();
    }

    /**
     * Check if an account is owned by this cluster.
     * This uses the allAccounts list which is updated by reshard operations.
     */
    public synchronized boolean isAccountOwned(String acctId) {
        if (acctId == null) return false;
        String key = acctId.trim();
        if (key.isEmpty()) return false;
        return allAccounts.contains(key);
    }

    public synchronized void applyAccountOwnershipDelta(java.util.Map<String,Integer> assign,
                                                         java.util.Set<String> drop) {
        java.util.Map<String,Integer> current = database.snapshot();
        java.util.LinkedHashSet<String> updatedAccounts = new java.util.LinkedHashSet<>(allAccounts);
        java.util.LinkedHashSet<String> movedIn = new java.util.LinkedHashSet<>();

        if (drop != null && !drop.isEmpty()) {
            for (String raw : drop) {
                if (raw == null) {
                    continue;
                }
                String key = raw.trim();
                if (key.isEmpty()) {
                    continue;
                }
                updatedAccounts.remove(key);
                current.remove(key);
            }
        }

        if (assign != null && !assign.isEmpty()) {
            for (java.util.Map.Entry<String,Integer> e : assign.entrySet()) {
                String raw = e.getKey();
                if (raw == null) {
                    continue;
                }
                String key = raw.trim();
                if (key.isEmpty()) {
                    continue;
                }
                Integer value = e.getValue();
                if (value == null) {
                    continue;
                }
                updatedAccounts.add(key);
                current.put(key, value);
                movedIn.add(key);
            }
        }

        allAccounts.clear();
        allAccounts.addAll(updatedAccounts);
        database.replaceWith(current);




        for (String key : movedIn) {
            recordModifiedKeys(null, key);
        }
    }

    public java.util.List<String> tryLockAccounts(java.util.List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return java.util.Collections.emptyList();
        }

        java.util.LinkedHashSet<String> unique = new java.util.LinkedHashSet<>();
        for (String k : keys) {
            if (k == null) continue;
            String t = k.trim();
            if (t.isEmpty()) continue;
            unique.add(t);
        }
        if (unique.isEmpty()) {
            return java.util.Collections.emptyList();
        }

        java.util.List<String> ordered = new java.util.ArrayList<>(unique);
        java.util.Collections.sort(ordered);

        java.util.List<String> acquired = new java.util.ArrayList<>(ordered.size());
        for (String key : ordered) {
            AtomicBoolean cell = lockTable.computeIfAbsent(key, kk -> new AtomicBoolean(false));
            if (!cell.compareAndSet(false, true)) {
                for (String held : acquired) {
                    AtomicBoolean c = lockTable.get(held);
                    if (c != null) {
                        c.set(false);
                    }
                }
                return null;
            }
            acquired.add(key);
        }
        return acquired;
    }

    public boolean isAccountLocked(String key) {
        if (key == null) return false;
        String t = key.trim();
        if (t.isEmpty()) return false;
        AtomicBoolean cell = lockTable.get(t);
        return cell != null && cell.get();
    }

    public void registerLocksForSeq(int seq, java.util.List<String> keys) {
        if (seq <= 0) return;
        if (keys == null || keys.isEmpty()) return;
        locksBySeq.put(seq, java.util.Collections.unmodifiableList(new java.util.ArrayList<>(keys)));
    }

    public void unlockAccounts(java.util.List<String> keys) {
        if (keys == null || keys.isEmpty()) return;
        for (String key : keys) {
            AtomicBoolean cell = lockTable.get(key);
            if (cell != null) {
                cell.set(false);
            }
        }
    }

    public void unlockBySeq(int seq) {
        java.util.List<String> keys = locksBySeq.remove(seq);
        if (keys == null || keys.isEmpty()) return;
        for (String key : keys) {
            AtomicBoolean cell = lockTable.get(key);
            if (cell != null) {
                cell.set(false);
            }
        }
    }

    public synchronized int getAndIncrNextSeq() {
        Integer reuse = recycledSeqs.pollFirst();
        if (reuse != null) {
            return reuse;
        }
        return nextSeq.getAndIncrement();
    }

    public int getOrAssignTwoPcSeq(TwoPcState txState) {
        if (txState == null) {
            throw new IllegalArgumentException("null TwoPcState");
        }
        synchronized (txState) {
            if (txState.hasLogSeq()) {
                return txState.getLogSeq();
            }
            int seq = getAndIncrNextSeq();
            txState.setLogSeqIfUnset(seq);
            return txState.getLogSeq();
        }
    }

    /**
     * Get or assign a separate sequence number for the COMMIT/ABORT phase of 2PC.
     * Per spec, PREPARE and COMMIT/ABORT are two separate consensus rounds.
     */
    public int getOrAssignTwoPcFinalSeq(TwoPcState txState) {
        if (txState == null) {
            throw new IllegalArgumentException("null TwoPcState");
        }
        synchronized (txState) {
            if (txState.hasFinalSeq()) {
                return txState.getFinalSeq();
            }
            int seq = getAndIncrNextSeq();
            txState.setFinalSeqIfUnset(seq);
            return txState.getFinalSeq();
        }
    }

    public int getNextExecSeq() { return nextExecSeq.get(); }
    public void bumpExecSeq(){ nextExecSeq.incrementAndGet(); }
    public BallotNum getCurrentBallot() { return currentBallot; }
    public BallotNum getHighestSeenBallot() { return highestSeenBallot; }
    public int getLastCheckpointSeq() { return lastCheckpointSeq.get(); }
    public String getLastCheckpointDigest() { return lastCheckpointDigest.get(); }
    public java.util.Map<String, Integer> getLastCheckpointSnapshot() { return lastCheckpointSnapshot; }
    public int getLastExecutedSeq() { return Math.max(0, lastExecutedSeq.get()); }

    public void recordExecution(int seq) {
        if (seq <= 0) return;
        lastExecutedSeq.updateAndGet(cur -> Math.max(cur, seq));
        if (!checkpointingEnabled) return;
        if (!getIsLeader()) return;
        if (checkpointPeriod <= 0) return;
        if (seq <= lastCheckpointSeq.get()) return;
        if (seq % checkpointPeriod != 0) return;
        synchronized (checkpointLock) {
            java.util.Map<String,Integer> snap = database.snapshot();
            stagedCheckpointSnapshot = new java.util.HashMap<>(snap);
            stagedCheckpointSeq.set(seq);
        }
        pendingCheckpointSeq.updateAndGet(prev -> Math.max(prev, seq));
    }


    public int pollPendingCheckpointSeq() {
        if (!checkpointingEnabled) return 0;
        return pendingCheckpointSeq.getAndSet(0);
    }

    public void updateCheckpointMetadata(int seq, String digest) {
        if (seq <= 0) return;
        lastCheckpointSeq.updateAndGet(cur -> Math.max(cur, seq));
        if (digest != null) {
            lastCheckpointDigest.set(digest);
        }
        pendingCheckpointSeq.updateAndGet(prev -> prev > seq ? prev : 0);
    }

    public void setLastCheckpointSnapshot(java.util.Map<String, Integer> snapshot) {
        if (snapshot == null) {
            lastCheckpointSnapshot = null;
        } else {
            lastCheckpointSnapshot = java.util.Collections.unmodifiableMap(new java.util.HashMap<>(snapshot));
        }
    }

    public synchronized java.util.Map<String,Integer> takeStagedCheckpointIfSeq(int seq) {
        if (stagedCheckpointSnapshot != null && stagedCheckpointSeq.get() == seq) {
            java.util.Map<String,Integer> out = new java.util.HashMap<>(stagedCheckpointSnapshot);
            stagedCheckpointSnapshot = null;
            stagedCheckpointSeq.set(0);
            return out;
        }
        return null;
    }


    public void applyCheckpointSnapshot(Map<String, Integer> snapshot, int seq, String digest) {
        if (!checkpointingEnabled || seq <= 0) {
            return;
        }
        synchronized (checkpointLock) {
            int already = lastExecutedSeq.get();
            if (already >= seq) {
                updateCheckpointMetadata(seq, digest);
                return;
            }
            database.replaceWith(snapshot);

            updateCheckpointMetadata(seq, digest);
            setLastCheckpointSnapshot(snapshot);

            lastExecutedSeq.updateAndGet(cur -> Math.max(cur, seq));

            advanceExecCursorIfNeeded(seq + 1);
            advanceNextSeqIfNeeded(seq);
            log.values().stream()
                    .filter(entry -> entry.getSeq() <= seq)
                    .forEach(entry -> {
                        entry.advancePhase(Phase.EXECUTED);
                        entry.getExecResult().complete(Boolean.TRUE);
                    });
        }
    }

    public void noteCheckpointFromPeer(int seq, String digest) {
        if (seq <= 0) return;
        synchronized (checkpointLock) {
            updateCheckpointMetadata(seq, digest);
            lastExecutedSeq.updateAndGet(cur -> Math.max(cur, seq));
            advanceExecCursorIfNeeded(seq + 1);
            advanceNextSeqIfNeeded(seq);
            log.values().stream()
                    .filter(entry -> entry.getSeq() <= seq)
                    .forEach(entry -> {
                        entry.advancePhase(Phase.EXECUTED);
                        entry.getExecResult().complete(Boolean.TRUE);
                    });
        }
    }


    public int pruneLogAfterNewView(int checkpointSeq, Set<Integer> keepSeqs) {
        if (keepSeqs == null) {
            keepSeqs = Collections.emptySet();
        }
        int highestKeep = checkpointSeq;
        for (int seq : keepSeqs) {
            if (seq > highestKeep) {
                highestKeep = seq;
            }
        }

        java.util.List<Integer> toRemove = new ArrayList<>();
        for (Map.Entry<Integer, LogEntry> entry : log.entrySet()) {
            int seq = entry.getKey();
            if (seq <= checkpointSeq) {
                continue;
            }
            if (keepSeqs.contains(seq)) {
                continue;
            }
            LogEntry le = entry.getValue();
            if (le.getPhase().ordinal() >= Phase.COMMITTED.ordinal()) {
                continue;
            }
            toRemove.add(seq);
        }

        for (Integer seq : toRemove) {
            LogEntry removed = log.remove(seq);
            if (removed != null) {
                removed.getExecResult().completeExceptionally(
                        new CancellationException("Pruned by new view"));
            }
        }

        if (!toRemove.isEmpty()) {
            advanceExecCursorIfNeeded(checkpointSeq + 1);
            advanceNextSeqIfNeeded(Math.max(highestKeep, checkpointSeq));
        }

        return toRemove.size();
    }

    public void setHighestSeenBallot(BallotNum b) {
        synchronized (ballotLock) {
            updateHighestSeenLocked(b);
            if (b != null && (currentBallot == null || b.ge(currentBallot))) {
                currentBallot = b;
            }
        }
    }

    public boolean getIsLeader() { return isLeader.get(); }

    public void setLeader(boolean leader) { this.isLeader.set(leader); }

    public void setCurrentBallot(BallotNum b) {
        synchronized (ballotLock) {
            this.currentBallot = b;
            updateHighestSeenLocked(b);
        }
    }

    public void noteHigherBallot(BallotNum b) {
        synchronized (ballotLock) {
            updateHighestSeenLocked(b);
            if (b != null && (currentBallot == null || b.ge(currentBallot))) {
                currentBallot = b;
            }
        }
    }

    public MessageAudit getAudit() { return audit; }

    public BallotNum allocateNextBallot(BallotNum externalMax) {
        synchronized (ballotLock) {
            int curRound = (currentBallot == null) ? 0 : currentBallot.getRound();
            int extRound = (externalMax == null) ? 0 : externalMax.getRound();
            int newRound = Math.max(curRound, extRound) + 1;
            BallotNum next = new BallotNum(newRound, nodeConfig.getNodeId());
            this.currentBallot = next;
            updateHighestSeenLocked(next);
            return next;
        }
    }


    public boolean promiseBallot(BallotNum ballot) {
        synchronized (ballotLock) {

            BallotNum current = this.currentBallot;
            if (current != null && ballot.compareTo(current) <= 0) {
                return false;
            }
            currentBallot = ballot;
            updateHighestSeenLocked(ballot);
            return true;
        }
    }

    public boolean acceptBallot(BallotNum ballot) {
        synchronized (ballotLock) {

            if (currentBallot != null && ballot.compareTo(currentBallot) < 0) {
                return false;
            }
            if (currentBallot == null || ballot.compareTo(currentBallot) > 0) {
                currentBallot = ballot;
            }
            updateHighestSeenLocked(ballot);
            return true;
        }
    }

    public void learnLeader(BallotNum ballot, int leaderId) {
        synchronized (ballotLock) {
            boolean newer = ballot != null && (highestSeenBallot == null || ballot.ge(highestSeenBallot));
            updateHighestSeenLocked(ballot);
            if (ballot != null && (currentBallot == null || ballot.ge(currentBallot))) {
                currentBallot = ballot;
            }
            if (newer && leaderId != 0) {
                knownLeaderId = leaderId;
                if (leaderId != nodeConfig.getNodeId()) {
                    isLeader.set(false);
                }
            }
        }
    }

    public int getKnownLeaderId() { return knownLeaderId; }
    public void setKnownLeaderId(int id) {
        knownLeaderId = id;
        if (id != 0 && id != nodeConfig.getNodeId()) {
            isLeader.set(false);
        }
    }

    public Set<Integer> setLivePeerIds(Set<Integer> peers) {
        Set<Integer> previous = livePeerIds;
        Set<Integer> next;
        if (peers == null || peers.isEmpty()) {
            next = Collections.emptySet();
        } else {
            next = Collections.unmodifiableSet(new LinkedHashSet<>(peers));
        }
        livePeerIds = next;
        boolean allowSelf = next.isEmpty() || next.contains(nodeConfig.getNodeId());
        selfAllowed = allowSelf;
        if (!allowSelf) {
            isLeader.set(false);
        }
        if (!next.isEmpty() && (knownLeaderId != 0 && !next.contains(knownLeaderId))) {
            knownLeaderId = 0;
        }
        return computeNewlyEnabledPeers(previous, next);
    }

    public boolean isPeerAllowed(int peerId) {
        Set<Integer> current = livePeerIds;
        return current.isEmpty() || current.contains(peerId);
    }

    public boolean isSelfAllowed() {
        return selfAllowed;
    }

    private Set<Integer> computeNewlyEnabledPeers(Set<Integer> previous, Set<Integer> current) {
        Set<Integer> prevEffective = expandPeerFilter(previous);
        Set<Integer> currentEffective = expandPeerFilter(current);
        if (currentEffective.isEmpty()) {
            return Collections.emptySet();
        }
        currentEffective.removeAll(prevEffective);
        if (currentEffective.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(currentEffective);
    }

    private Set<Integer> expandPeerFilter(Set<Integer> raw) {
        if (raw == null || raw.isEmpty()) {
            return new LinkedHashSet<>(nodeConfig.getPeers().keySet());
        }
        return new LinkedHashSet<>(raw);
    }

    public void advanceNextSeqIfNeeded(int seq) {
        nextSeq.updateAndGet(cur -> Math.max(cur, seq + 1));
    }

    public void advanceExecCursorIfNeeded(int seq) {
        nextExecSeq.updateAndGet(cur -> Math.max(cur, seq));
    }

    private void updateHighestSeenLocked(BallotNum ballot) {
        if (ballot == null) return;
        BallotNum hs = this.highestSeenBallot;
        if (hs == null || ballot.ge(hs)) {
            this.highestSeenBallot = ballot;
        }
    }


    public boolean isLfSuspended() {
        return lfSuspended;
    }

    public void setLfSuspended(boolean lfSuspended) {
        this.lfSuspended = lfSuspended;
    }

    public boolean isTimersPaused() {
        return timersPaused;
    }

    public void setTimersPaused(boolean timersPaused) {
        this.timersPaused = timersPaused;
    }
}
