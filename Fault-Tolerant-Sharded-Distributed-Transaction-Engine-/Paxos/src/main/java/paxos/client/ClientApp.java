package paxos.client;

import paxos.proto.NodeServiceGrpc;
import paxos.proto.PaxosProto.*;
import paxos.utils.CSVUtil;
import paxos.utils.CSVUtil.*;
import paxos.utils.PrintHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ClientApp implements AutoCloseable {

    private final ClusterCoordinator cluster;
    public static final String OVERRIDE_MARKER = "OVERRIDE_WORKED_123";
    private final ReshardPerformer reshardPerformer;
    private final ConcurrentHashMap<String, AtomicLong> clocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, paxos.client.ClientApp.ClientTimer> timers = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> perfStartTimes = new ConcurrentHashMap<>();
    private final paxos.client.ClientApp.PerfMetrics perfStats = new paxos.client.ClientApp.PerfMetrics();
    private volatile boolean benchmarkModeActive = false;
    private volatile boolean benchmarkAutoAggressive = false;

    private static final boolean PAUSE_BETWEEN_TWO_SETS = Boolean.parseBoolean(System.getProperty("pauseBetweenSets", "true"));
    private static final boolean PAUSE_TIMERS_BETWEEN_SETS = Boolean.parseBoolean(System.getProperty("pauseTimersBetweenSets", "true"));
    private static final boolean PERFORMANCE_MODE = Boolean.parseBoolean(System.getProperty("performanceMode", "false"));
    private static final boolean BENCHMARK_AGGRESSIVE_MODE = Boolean.parseBoolean(System.getProperty("benchmarkAggressive", "false"));
    private static final long BENCHMARK_AGGRESSIVE_TIMEOUT = Long.getLong(
            "benchmarkAggressiveTimeout",
            TimeUnit.MICROSECONDS.toNanos(500L));
    private static final Scanner STDIN = new Scanner(System.in);

    private static final long CLIENT_TIMEOUT = Long.getLong(
            "clientTimeoutMs",
            1200L);
    private static final long CLIENT_RETRY_BACKOFF = Long.getLong(
            "clientRetryBackoffMs",
            PERFORMANCE_MODE ? 50L : 150L);
    private static final int CLIENT_MAX_ATTEMPTS = Integer.getInteger(
            "clientMaxAttempts",
            PERFORMANCE_MODE ? 10 : 20);
    private static final long CLUSTER_SYNC_TIMEOUT = Long.getLong("clusterSyncTimeoutMs", 1000L);

    private static final ScheduledExecutorService BG_TIMER_CLIENT = Executors.newScheduledThreadPool(4, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "client timer");
            t.setDaemon(true);
            return t;
        }
    });

    private static final ExecutorService BG_TIMER_CLIENT_RPC = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            r -> { Thread t = new Thread(r, "client rpc timer"); t.setDaemon(true); return t; });

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> BG_TIMER_CLIENT.shutdownNow(), "client timer shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> BG_TIMER_CLIENT_RPC.shutdownNow(), "client rpc timer shutdown"));
    }

    private void resetClientStateForNewSet() {
        log("resetClientStateForNewSet: clearing client clocks and timers for new set");


        clocks.clear();
        timers.values().forEach(paxos.client.ClientApp.ClientTimer::stop);
        timers.clear();
    }


    private Reply sendOnce(ClientRequest req) {
        Reply r = sendToLeaderFirst(req);
        if (r != null && isDefinitiveReply(r)) return r;
        paxos.client.ClientApp.submitResp out = broadcastRequestToAllNodes(req);
        if (out.finalReply == null) {
            log("sendOnce: no definitive reply for ts=%d %s->%s amt=%d hadAnyReply=%s%n",
                    req.getTimestamp(), req.getSender(), req.getReceiver(), req.getAmount(),
                    out.hadAnyReply);
        }
        return out.finalReply;
    }

    static NodeServiceGrpc.NodeServiceBlockingStub timeout(NodeServiceGrpc.NodeServiceBlockingStub stub) {
        return stub.withDeadlineAfter(CLIENT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    public ClientApp(String bootstrapTarget, Collection<String> peerTargets) {
        this.cluster = new ClusterCoordinator(bootstrapTarget, peerTargets);
        this.reshardPerformer = new ReshardPerformer(this.cluster, CLUSTER_SYNC_TIMEOUT);
    }

    @Override
    public void close() {
        cluster.close();
    }

    private long getNextTs(String clientId) {
        return clocks.computeIfAbsent(clientId, k -> new AtomicLong(0)).incrementAndGet();
    }

    private void printReplies(ClientRequest req, Reply rep) {
        if (rep == null) return;

        boolean isReadOnlyReq = req != null
                && req.getAmount() == 0
                && (req.getReceiver() == null || req.getReceiver().isEmpty())
                && req.getSender() != null && !req.getSender().isEmpty();



        if (isReadOnlyReq && isDefinitiveReply(rep)) {
            String acc = req.getSender();
            System.out.printf("Balance(%s) = %s%n", acc, rep.getMessage());
        }

        if (rep.hasLeaderBallot()) {
            log("Reply ts=%d ok=%s msg=%s leader=(%d,%d)%n",
                    rep.getTimestamp(), rep.getSuccess(), rep.getMessage(),
                    rep.getLeaderBallot().getRound(), rep.getLeaderBallot().getNodeId());
        } else {
            log("Reply ts=%d ok=%s msg=%s%n",
                    rep.getTimestamp(), rep.getSuccess(), rep.getMessage());
        }
    }
    public void submit(String clientId, String sender, String receiver, int amount) {
        long ts = getNextTs(clientId);
        long startNs = System.nanoTime();
        perfOnNewRequest(clientId, ts, startNs);

        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setTimestamp(ts)
                .setSender(sender)
                .setReceiver(receiver)
                .setAmount(amount)
                .build();

        if (useAggressiveBenchmarkMode()) {
            submitAggressiveBenchmark(clientId, ts, startNs, req);
            return;
        }

        paxos.client.ClientApp.ClientTimer timer = timers.computeIfAbsent(clientId, id -> new paxos.client.ClientApp.ClientTimer());
        int attempt = 0;

        for(;;) {
            attempt++;
            timer.start();

            Reply rep = sendToLeaderFirst(req);
            if (rep != null && isDefinitiveReply(rep)) {
                timer.stop();
                perfOnReply(clientId, ts);
                return;
            }

            long wait = timer.remainingTime();
            if (wait > 0) stayIdleFor(wait);

            paxos.client.ClientApp.submitResp outcome = broadcastRequestToAllNodes(req);
            timer.stop();
            if (outcome.finalReply != null) {
                perfOnReply(clientId, ts);
                return;
            }


            if (CLIENT_MAX_ATTEMPTS > 0 && attempt >= CLIENT_MAX_ATTEMPTS) {
                log("Client %s ts=%d: reached max attempts (%d); giving up for this set%n", clientId, ts, CLIENT_MAX_ATTEMPTS);
                return;
            }

            if (CLIENT_RETRY_BACKOFF > 0) {
                stayIdleFor(CLIENT_RETRY_BACKOFF);
            }
        }
    }

    private void submitAggressiveBenchmark(String clientId, long ts, long startNs, ClientRequest req) {
        long budgetNs = Math.max(0L, BENCHMARK_AGGRESSIVE_TIMEOUT);
        if (budgetNs == 0L) {
            log("Aggressive benchmark: budgetNs is 0; skipping txn ts=%d", ts);
            return;
        }
        long deadlineNs = startNs + budgetNs;

        Reply rep = sendToLeaderFirst(req, deadlineNs);
        if (rep != null && isDefinitiveReply(rep)) {
            perfOnReply(clientId, ts);
            return;
        }

        if (System.nanoTime() < deadlineNs) {
            paxos.client.ClientApp.submitResp outcome = broadcastRequestToAllNodes(req, deadlineNs);
            if (outcome.finalReply != null) {
                perfOnReply(clientId, ts);
                return;
            }
        }

        log("Aggressive benchmark: skipping txn ts=%d after %.3f ms budget without definitive reply",
                ts, budgetNs / 1_000_000.0);
        perfOnSkip(clientId, ts, Math.max(System.nanoTime(), deadlineNs));
    }

    private boolean useAggressiveBenchmarkMode() {
        return benchmarkModeActive && (BENCHMARK_AGGRESSIVE_MODE || benchmarkAutoAggressive);
    }

    private String routingKeyFor(ClientRequest req) {
        if (req.getSender() != null && !req.getSender().isEmpty()) return req.getSender();
        if (req.getReceiver() != null && !req.getReceiver().isEmpty()) return req.getReceiver();
        return "";
    }

    private Reply sendToLeaderFirst(ClientRequest req) {
        return sendToLeaderFirst(req, 0L);
    }

    private Reply sendToLeaderFirst(ClientRequest req, long deadlineNs) {
        String key = routingKeyFor(req);
        NodeServiceGrpc.NodeServiceBlockingStub leader = cluster.getPrefStubForAccount(key);
        if (leader == null) return null;
        Reply r = submitToStub(leader, req, deadlineNs);
        if (r != null) {
            cluster.rememberLeader(r);
            printReplies(req, r);
        }
        return r;
    }

    private paxos.client.ClientApp.submitResp broadcastRequestToAllNodes(ClientRequest req) {
        return broadcastRequestToAllNodes(req, 0L);
    }

    private paxos.client.ClientApp.submitResp broadcastRequestToAllNodes(ClientRequest req, long deadlineNs) {
        String key = routingKeyFor(req);
        var stubsList = cluster.constructNodeOrderForAccount(key);
        boolean hadReply = false;
        Reply finalRep = null;

        var cs = new ExecutorCompletionService<Reply>(BG_TIMER_CLIENT_RPC);
        stubsList.forEach(stub -> cs.submit(() -> submitToStub(stub, req, deadlineNs)));

        long deadlineTime = deadlineNs > 0L
                ? deadlineNs
                : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLIENT_TIMEOUT);
        int remaining = stubsList.size();

        while (remaining-- > 0) {
            long waitTime = deadlineTime - System.nanoTime();
            if (waitTime <= 0) break;
            try {
                var f = cs.poll(waitTime, TimeUnit.NANOSECONDS);
                if (f == null) break;
                Reply r = f.get();
                if (r != null) {
                    hadReply = true;
                    cluster.rememberLeader(r);
                    printReplies(req, r);
                    if (isDefinitiveReply(r)) { finalRep = r; break; }
                }
            }
            catch (Exception ignore) {

            }
        }

        if (!hadReply) {
            log("broadcastRequestToAllNodes: no replies for ts=%d %s->%s amt=%d targets=%d%n",
                    req.getTimestamp(), req.getSender(), req.getReceiver(), req.getAmount(),
                    stubsList.size());
        } else if (finalRep == null) {
            log("broadcastRequestToAllNodes: replies but none definitive for ts=%d %s->%s amt=%d%n",
                    req.getTimestamp(), req.getSender(), req.getReceiver(), req.getAmount());
        }

        return new paxos.client.ClientApp.submitResp(finalRep, hadReply);
    }


    private Reply submitToStub(NodeServiceGrpc.NodeServiceBlockingStub stub, ClientRequest req) {
        return submitToStub(stub, req, 0L);
    }

    private Reply submitToStub(NodeServiceGrpc.NodeServiceBlockingStub stub, ClientRequest req, long deadlineNs) {
        if (stub == null) return null;
        try {
            NodeServiceGrpc.NodeServiceBlockingStub effectiveStub;
            if (deadlineNs > 0L) {
                long remaining = deadlineNs - System.nanoTime();
                if (remaining <= 0L) {
                    return null;
                }
                effectiveStub = stub.withDeadlineAfter(Math.max(1L, remaining), TimeUnit.NANOSECONDS);
            } else {
                effectiveStub = timeout(stub);
            }
            return effectiveStub.submit(req);
        }
        catch (Exception ex) {
            log("submitToStub RPC failure for ts=%d %s->%s amt=%d ex=%s%n",
                    req.getTimestamp(), req.getSender(), req.getReceiver(), req.getAmount(), ex.toString());
            return null;
        }
    }


    private static boolean isDefinitiveReply(Reply reply) {
        return reply!=null && (reply.getSuccess() || reply.getResultType().equals(Reply.ResultType.FAILURE));
    }

    private static void stayIdleFor(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public void printDB() { cluster.printDB(); }
    public void printDB(Integer nodeId) { cluster.printDB(nodeId); }
    public void printLogs() { cluster.printLogs(); }
    public void printLogs(Integer nodeId) { cluster.printLogs(nodeId); }
    public void printStatus(int seq) { cluster.printStatus(seq); }
    public void printStatus(int seq, Integer nodeId) { cluster.printStatus(seq, nodeId); }
    public void printStatusSummary() { cluster.printStatusSummary(); }
    public void printView() { cluster.printView(); }
    public void printView(Integer nodeId) { cluster.printView(nodeId); }

    public void printBalance(int accountId) { cluster.printBalance(accountId); }
    public void printAudit() { cluster.printAudit(); }
    public void printAudit(Integer nodeId) { cluster.printAudit(nodeId); }

    public void printReshard() { reshardPerformer.runReshardAndPrint(); }

    public void printPerformance() { perfStats.printSummary(); }

    public void resetPerformance() {
        perfStartTimes.clear();
        perfStats.reset();
    }

    private static String perfKey(String clientId, long ts) {
        return clientId + "#" + ts;
    }

    private void perfOnNewRequest(String clientId, long ts, long startNs) {
        if (clientId == null) return;
        if (ts <= 0L || startNs <= 0L) return;
        String key = perfKey(clientId, ts);
        perfStartTimes.put(key, startNs);
        perfStats.onStart(startNs);
    }

    private void perfOnReply(String clientId, long ts) {
        if (clientId == null) return;
        if (ts <= 0L) return;
        String key = perfKey(clientId, ts);
        Long startNs = perfStartTimes.get(key);
        if (startNs == null || startNs <= 0L) return;
        long endNs = System.nanoTime();
        if (endNs <= startNs) return;
        perfStats.onComplete(startNs, endNs);
    }

    private void perfOnSkip(String clientId, long ts, long endNs) {
        if (clientId == null) return;
        if (ts <= 0L) return;
        String key = perfKey(clientId, ts);
        Long startNs = perfStartTimes.get(key);
        if (startNs == null || startNs <= 0L) return;
        if (endNs <= startNs) {
            endNs = startNs + 1L;
        }
        perfStats.onComplete(startNs, endNs);
    }

    private void applyLiveNodes(List<String> liveNodes) {
        cluster.setLiveNodes(liveNodes);
    }

    private void resetViewHistoryForCurrentSet() {
        cluster.resetViewHistoryForCurrentSet();
    }


    public void runCsvConcurrent(Path csv) throws IOException, InterruptedException {
        List<TestSet> sets = CSVUtil.parse(csv);

        for (TestSet set : sets) {
            System.out.printf("%n=== Ready to run Set #%d: %d txns %s ===%n",
                    set.setNo(),
                    set.txns().size(),
                    set.liveNodes().isEmpty() ? "" : (", live=" + set.liveNodes()));

            processCurrentSet(set.setNo());

            resetPerformance();
            log("BEGIN set #%d: resetting client and server state for new set", set.setNo());
            resetClientStateForNewSet();


            resetViewHistoryForCurrentSet();




            applyLiveNodes(set.liveNodes());

            cluster.resumeAfterSetAll();

            cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);

            java.util.LinkedHashSet<Integer> currentLive = initLiveNodeSet(set.liveNodes());

            List<List<Txn>> segments = set.segments();
            if (segments == null || segments.isEmpty()) {
                segments = List.of(set.txns());
            }

            for (int sIdx = 0; sIdx < segments.size(); sIdx++) {
                List<Txn> segment = segments.get(sIdx);

                int idx = 0;
                int n = segment.size();
                while (idx < n) {
                    List<Txn> batch = new ArrayList<>();
                    while (idx < n && !isFailOrRecoverCommand(segment.get(idx))) {
                        batch.add(segment.get(idx++));
                    }
                    if (!batch.isEmpty()) {
                        runTxnBatch(batch, sIdx);
                    }
                    if (idx < n) {
                        Txn cmd = segment.get(idx++);
                        currentLive = applyFailOrRecoverCommand(cmd, set.setNo(), currentLive);
                    }
                }

                if (sIdx < segments.size() - 1) {
                    cluster.suspendLeaderForSet();
                }
            }

            cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);
            if (PAUSE_TIMERS_BETWEEN_SETS) {
                cluster.pauseTimersAll();
                cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);
            }

            log("END set #%d: cluster quiesced and timers paused after set", set.setNo());
        }

        if (PAUSE_TIMERS_BETWEEN_SETS) {
            cluster.resumeAfterSetAll();
            cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);
            cluster.pauseTimersAll();
            cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);
        }
    }

    private static final class PerfMetrics {
        private final AtomicLong firstStartNs = new AtomicLong(0L);
        private final AtomicLong lastEndNs = new AtomicLong(0L);
        private final AtomicLong completedCount = new AtomicLong(0L);
        private final AtomicLong totalLatencyNs = new AtomicLong(0L);
        private final AtomicLong minLatencyNs = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxLatencyNs = new AtomicLong(0L);

        void onStart(long startNs) {
            if (startNs <= 0L) return;
            firstStartNs.updateAndGet(prev -> (prev == 0L || startNs < prev) ? startNs : prev);
        }

        void onComplete(long startNs, long endNs) {
            if (startNs <= 0L || endNs <= startNs) return;
            long dur = endNs - startNs;
            completedCount.incrementAndGet();
            totalLatencyNs.addAndGet(dur);
            minLatencyNs.updateAndGet(prev -> Math.min(prev, dur));
            maxLatencyNs.updateAndGet(prev -> Math.max(prev, dur));
            lastEndNs.updateAndGet(prev -> Math.max(prev, endNs));
        }

        void reset() {
            firstStartNs.set(0L);
            lastEndNs.set(0L);
            completedCount.set(0L);
            totalLatencyNs.set(0L);
            minLatencyNs.set(Long.MAX_VALUE);
            maxLatencyNs.set(0L);
        }

        void printSummary() {
            long count = completedCount.get();
            if (count == 0L) {
                System.out.println("Performance: no completed transactions yet");
                return;
            }
            long first = firstStartNs.get();
            long last = lastEndNs.get();
            long elapsedNs = (first > 0L && last > first) ? (last - first) : 0L;

            double avgLatMs = (totalLatencyNs.get() / (double) count) / 1_000_000.0;
            double minLatMs = (minLatencyNs.get() == Long.MAX_VALUE ? 0.0 : minLatencyNs.get() / 1_000_000.0);
            double maxLatMs = maxLatencyNs.get() / 1_000_000.0;
            double throughput = (elapsedNs > 0L)
                    ? (count * 1_000_000_000.0 / (double) elapsedNs)
                    : 0.0;

            System.out.printf(
                    "Performance: completed=%d, avgLatency=%.2f ms, minLatency=%.2f ms, maxLatency=%.2f ms, throughput=%.2f txns/sec%n",
                    count, avgLatMs, minLatMs, maxLatMs, throughput);
        }
    }

    private java.util.LinkedHashSet<Integer> initLiveNodeSet(List<String> liveNodes) {
        java.util.LinkedHashSet<Integer> set = new java.util.LinkedHashSet<>();
        if (liveNodes == null || liveNodes.isEmpty()) {
            for (int i = 1; i <= 9; i++) {
                set.add(i);
            }
            return set;
        }
        for (String entry : liveNodes) {
            int id = parseNodeIdToken(entry);
            if (id > 0) {
                set.add(id);
            }
        }
        return set;
    }

    private static int parseNodeIdToken(String token) {
        if (token == null) return 0;
        String digits = token.replaceAll("[^0-9]", "").trim();
        if (digits.isEmpty()) return 0;
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static boolean isFailOrRecoverCommand(Txn t) {
        if (t == null) return false;
        if (t.receiver() != null && !t.receiver().isEmpty()) return false;
        if (t.amount() != 0) return false;
        String s = t.sender();
        if (s == null || s.isEmpty()) return false;
        String up = s.trim().toUpperCase(Locale.ROOT);
        return up.startsWith("F") || up.startsWith("R");
    }

    private static boolean isFailCommand(Txn t) {
        if (!isFailOrRecoverCommand(t)) return false;
        String s = t.sender();
        if (s == null) return false;
        String up = s.trim().toUpperCase(Locale.ROOT);
        return up.startsWith("F");
    }

    private static boolean isRecoverCommand(Txn t) {
        if (!isFailOrRecoverCommand(t)) return false;
        String s = t.sender();
        if (s == null) return false;
        String up = s.trim().toUpperCase(Locale.ROOT);
        return up.startsWith("R");
    }

    private java.util.LinkedHashSet<Integer> applyFailOrRecoverCommand(Txn cmd, int setNo, java.util.LinkedHashSet<Integer> currentLive) {
        if (!isFailOrRecoverCommand(cmd)) {
            return currentLive;
        }
        if (currentLive == null || currentLive.isEmpty()) {
            currentLive = initLiveNodeSet(java.util.Collections.emptyList());
        }
        int nodeId = parseNodeIdToken(cmd.sender());
        if (nodeId <= 0) {
            log("Ignoring malformed F/R command '%s' in set %d%n", cmd.sender(), setNo);
            return currentLive;
        }
        java.util.LinkedHashSet<Integer> next = new java.util.LinkedHashSet<>(currentLive);
        if (isFailCommand(cmd)) {
            next.remove(nodeId);
        } else if (isRecoverCommand(cmd)) {
            next.add(nodeId);
        }
        java.util.List<String> liveTokens;
        if (next.isEmpty()) {
            liveTokens = java.util.List.of();
        } else {
            java.util.List<Integer> ordered = new java.util.ArrayList<>(next);
            java.util.Collections.sort(ordered);
            java.util.List<String> tmp = new java.util.ArrayList<>(ordered.size());
            for (int id : ordered) {
                tmp.add("n" + id);
            }
            liveTokens = java.util.Collections.unmodifiableList(tmp);
        }
        applyLiveNodes(liveTokens);
        return next;
    }

    private void runTxnBatch(List<Txn> batch, int segmentIndex) throws InterruptedException {
        Map<String, List<Txn>> byClient = new LinkedHashMap<>();
        for (Txn t : batch) {
            if (isFailOrRecoverCommand(t)) continue;
            byClient.computeIfAbsent(t.sender(), k -> new ArrayList<>()).add(t);
            reshardPerformer.noteTxn(t.sender(), t.receiver(), t.amount());
        }
        if (byClient.isEmpty()) return;

        List<Thread> clientJobThreads = new ArrayList<>();
        for (Map.Entry<String, List<Txn>> entry : byClient.entrySet()) {
            String clientId = entry.getKey();
            List<Txn> list = entry.getValue();
            Thread th = new Thread(() -> {
                list.forEach(t -> submit(clientId, t.sender(), t.receiver(), t.amount()));
            }, "client-" + clientId + "-seg" + segmentIndex);
            th.start();
            clientJobThreads.add(th);
        }

        for (Thread th : clientJobThreads) {
            try {
                th.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            }
        }
    }

    // I took help of chat gpt to generate CLI interface for processing transactions
    private void processCurrentSet(int setNo) {
        if (!PAUSE_BETWEEN_TWO_SETS) return;
        while (true) {
            System.out.print("Command before Set #" + setNo + " [enter=run, db [n], balance <id>, log [n], audit [n], status <seq> [n], status, view [n], reshard, performance [reset], help]: ");
            final String line;
            try {
                line = STDIN.nextLine();
            } catch (NoSuchElementException ignored) {
                return;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.equalsIgnoreCase("run") || trimmed.equalsIgnoreCase("start") || trimmed.equalsIgnoreCase("go")) {
                return;
            }
            String[] parts = trimmed.split("\\s+");
            String cmd = parts[0].toLowerCase(Locale.ROOT);
            switch (cmd) {
                case "db":
                    if (parts.length >= 2) {
                        try { printDB(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printDB(); }
                    } else { printDB(); }
                    break;
                case "balance":
                    if (parts.length >= 2) {
                        try {
                            int id = Integer.parseInt(parts[1]);
                            printBalance(id);
                        } catch (NumberFormatException nfe) {
                            System.out.println("balance command requires an integer id: balance <id>");
                        }
                    } else {
                        System.out.println("balance command requires an integer id: balance <id>");
                    }
                    break;
                case "log":
                    if (parts.length >= 2) {
                        try { printLogs(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printLogs(); }
                    } else { printLogs(); }
                    break;
                case "audit":
                    if (parts.length >= 2) {
                        try { printAudit(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printAudit(); }
                    } else { printAudit(); }
                    break;
                case "status":
                    if (parts.length == 1 || parts[1].equalsIgnoreCase("summary")) {
                        printStatusSummary();
                    } else {
                        try {
                            int seq = Integer.parseInt(parts[1]);
                            if (parts.length >= 3) {
                                try { printStatus(seq, Integer.parseInt(parts[2])); }
                                catch (NumberFormatException nfe) { printStatus(seq); }
                            } else { printStatus(seq); }
                        } catch (NumberFormatException nfe) {
                            System.out.println("Status command requires an integer sequence: status <seq>");
                        }
                    }
                    break;
                case "performance":
                case "perf":
                    if (parts.length >= 2 && parts[1].equalsIgnoreCase("reset")) {
                        resetPerformance();
                        System.out.println("Performance stats reset.");
                    } else {
                        printPerformance();
                    }
                    break;
                case "view":
                    if (parts.length >= 2) {
                        try { printView(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printView(); }
                    } else { printView(); }
                    break;
                case "reshard":
                    printReshard();
                    break;
                case "help":
                    System.out.println("Commands: db [n], log [n], audit [n], status <seq> [n], status (summary), view [n], reshard, performance [reset], help, or press Enter to run the next set.");
                    break;
                default:
                    System.out.println("Unknown command. Type 'help' for options or press Enter to continue.");
            }
        }
    }

    private void keepConsoleAlive() {
        while (true) {
            System.out.print("Command [db [n], balance <id>, log [n], audit [n], status <seq> [n], status, view [n], reshard, exit]: ");
            final String line;
            try {
                line = STDIN.nextLine();
            } catch (NoSuchElementException ignored) {
                return;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            String[] parts = trimmed.split("\\s+");
            String cmd = parts[0].toLowerCase(Locale.ROOT);
            if (cmd.equals("exit") || cmd.equals("quit")) return;
            switch (cmd) {
                case "db":
                    if (parts.length >= 2) {
                        try { printDB(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printDB(); }
                    } else { printDB(); }
                    break;
                case "log":
                    if (parts.length >= 2) {
                        try { printLogs(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printLogs(); }
                    } else { printLogs(); }
                    break;
                case "audit":
                    if (parts.length >= 2) {
                        try { printAudit(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printAudit(); }
                    } else { printAudit(); }
                    break;
                case "status":
                    if (parts.length == 1 || parts[1].equalsIgnoreCase("summary")) {
                        printStatusSummary();
                    } else {
                        try {
                            int seq = Integer.parseInt(parts[1]);
                            if (parts.length >= 3) {
                                try { printStatus(seq, Integer.parseInt(parts[2])); }
                                catch (NumberFormatException nfe) { printStatus(seq); }
                            } else { printStatus(seq); }
                        } catch (NumberFormatException nfe) {
                            System.out.println("Status command requires an integer sequence: status <seq>");
                        }
                    }
                    break;
                case "view":
                    if (parts.length >= 2) {
                        try { printView(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printView(); }
                    } else { printView(); }
                    break;
                case "reshard":
                    printReshard();
                    break;
                case "performance":
                case "perf":
                    printPerformance();
                    break;
                default:
                    System.out.println("Unknown command. Try: db, log, audit, status <seq>, status (summary), view, exit");
            }
        }
    }

    private static final class submitResp {
        private final Reply finalReply;
        private final boolean hadAnyReply;

        private submitResp(Reply finalReply, boolean hadAnyReply) {
            this.finalReply = finalReply;
            this.hadAnyReply = hadAnyReply;
        }
    }

    private static final class ClientTimer {
        private final AtomicBoolean expired = new AtomicBoolean(false);
        private volatile ScheduledFuture<?> future;

        synchronized void start() {
            expired.set(false);
            cancel();
            future = BG_TIMER_CLIENT.schedule(() -> expired.set(true), CLIENT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        synchronized boolean stop() {
            cancel();
            return expired.get();
        }
        long remainingTime() {
            ScheduledFuture<?> f = future;
            if (f == null) return 0L;
            long nanos = f.getDelay(TimeUnit.NANOSECONDS);
            return nanos > 0 ? TimeUnit.NANOSECONDS.toMillis(nanos) : 0L;
        }
        private void cancel() {
            ScheduledFuture<?> f = future;
            if (f != null) f.cancel(false);
            future = null;
        }
    }

    private void log(String fmt, Object... args) {
        PrintHelper.debugf("[Client] " + fmt, args);
    }

    private static double clamp01(double v) {
        if (Double.isNaN(v)) return 0.0;
        if (v < 0.0) return 0.0;
        if (v > 1.0) return 1.0;
        return v;
    }

    private static double parseRatioArg(String raw) {
        if (raw == null || raw.isEmpty()) return 0.0;
        double v;
        try {
            v = Double.parseDouble(raw);
        } catch (NumberFormatException nfe) {
            return 0.0;
        }
        if (v > 1.0) {
            v = v / 100.0;
        }
        return clamp01(v);
    }

    private static final class SmallBankConfig {
        final long totalTx;
        final int numClients;
        final double readOnlyRatio;
        final double crossShardRatio;
        final double skew;
        final double hotsetFraction;
        final long seed;
        final int targetCluster;

        SmallBankConfig(long totalTx,
                        int numClients,
                        double readOnlyRatio,
                        double crossShardRatio,
                        double skew,
                        double hotsetFraction,
                        long seed,
                        int targetCluster) {
            this.totalTx = Math.max(0L, totalTx);
            this.numClients = Math.max(1, numClients);
            this.readOnlyRatio = clamp01(readOnlyRatio);
            this.crossShardRatio = clamp01(crossShardRatio);
            this.skew = clamp01(skew);
            this.hotsetFraction = clamp01(hotsetFraction);
            this.seed = seed;
            if (targetCluster >= 1 && targetCluster <= 3) {
                this.targetCluster = targetCluster;
            } else {
                this.targetCluster = 0;
            }
        }
    }

    private static void printSmallBankWorkloadInfo(paxos.client.ClientApp.SmallBankConfig cfg) {
        if (cfg == null) {
            System.out.println("SmallBank workload: <no config>");
            return;
        }
        long total = cfg.totalTx;
        long estReadOnly = Math.round(total * cfg.readOnlyRatio);
        long estReadWrite = total - estReadOnly;
        long estCrossShard = Math.round(estReadWrite * cfg.crossShardRatio);
        String clusterScope = (cfg.targetCluster >= 1 && cfg.targetCluster <= 3)
                ? ("C" + cfg.targetCluster)
                : "all";
        System.out.printf(
                "SmallBank workload: totalTx=%d, numClients=%d, readOnlyRatio=%.2f, crossShardRatio=%.2f, skew=%.2f, hotsetFraction=%.4f, seed=%d, targetCluster=%s%n",
                total, cfg.numClients, cfg.readOnlyRatio, cfg.crossShardRatio, cfg.skew, cfg.hotsetFraction, cfg.seed, clusterScope);
        System.out.printf(
                "SmallBank estimated counts: readOnly~%d, readWrite~%d, crossShardWrites~%d%n",
                estReadOnly, estReadWrite, estCrossShard);
    }

    private static final class TxnDesc {
        final String sender;
        final String receiver;
        final int amount;

        TxnDesc(String sender, String receiver, int amount) {
            this.sender = sender;
            this.receiver = receiver;
            this.amount = amount;
        }
    }

    private static final class SmallBankWorkload {
        private static final int MAX_ACCOUNT_ID = 9000;

        private final ClusterCoordinator cluster;
        private final paxos.client.ClientApp.SmallBankConfig cfg;
        private final int numAccounts;
        private final java.util.List<Integer> allAccounts;
        private final java.util.List<Integer> hotAccounts;
        private final java.util.List<Integer>[] accountsByCluster;
        private final java.util.List<Integer>[] hotAccountByCluster;

        @SuppressWarnings("unchecked")
        SmallBankWorkload(ClusterCoordinator cluster, paxos.client.ClientApp.SmallBankConfig cfg) {
            this.cluster = cluster;
            this.cfg = cfg;
            this.numAccounts = MAX_ACCOUNT_ID / 2;

            java.util.List<Integer> all = new java.util.ArrayList<>(numAccounts);
            java.util.List<Integer>[] byCluster = (java.util.List<Integer>[]) new java.util.List[4];
            java.util.List<Integer>[] hotByCluster = (java.util.List<Integer>[]) new java.util.List[4];
            for (int c = 1; c <= 3; c++) {
                byCluster[c] = new java.util.ArrayList<>();
                hotByCluster[c] = new java.util.ArrayList<>();
            }

            for (int cust = 1; cust <= numAccounts; cust++) {
                int checkingId = checkingIdForAccount(cust);
                String key = Integer.toString(checkingId);
                int clusterId = cluster.clusterIdForAccount(key);
                if (clusterId >= 1 && clusterId <= 3) {
                    if (cfg.targetCluster != 0 && clusterId != cfg.targetCluster) {
                        continue;
                    }
                    all.add(cust);
                    byCluster[clusterId].add(cust);
                }
            }

            this.accountsByCluster = byCluster;

            if (all.isEmpty()) {
                this.allAccounts = java.util.Collections.emptyList();
                this.hotAccounts = java.util.Collections.emptyList();
                for (int c = 1; c <= 3; c++) {
                    hotByCluster[c] = java.util.Collections.emptyList();
                }
                this.hotAccountByCluster = hotByCluster;
                return;
            }

            this.allAccounts = java.util.Collections.unmodifiableList(all);
            int hotCount = Math.max(1, (int) Math.round(all.size() * cfg.hotsetFraction));
            if (hotCount > all.size()) {
                hotCount = all.size();
            }
            java.util.List<Integer> hot = new java.util.ArrayList<>(all.subList(0, hotCount));
            this.hotAccounts = java.util.Collections.unmodifiableList(hot);

            java.util.Set<Integer> hotSet = new java.util.HashSet<>(this.hotAccounts);
            for (int c = 1; c <= 3; c++) {
                java.util.List<Integer> src = byCluster[c];
                if (src.isEmpty()) {
                    hotByCluster[c] = java.util.Collections.emptyList();
                } else {
                    java.util.List<Integer> hList = new java.util.ArrayList<>();
                    for (int cust : src) {
                        if (hotSet.contains(cust)) {
                            hList.add(cust);
                        }
                    }
                    if (hList.isEmpty()) {
                        int limit = Math.min(src.size(), Math.max(1, hotCount / 3));
                        hList.addAll(src.subList(0, limit));
                    }
                    hotByCluster[c] = java.util.Collections.unmodifiableList(hList);
                }
            }
            this.hotAccountByCluster = hotByCluster;
        }

        paxos.client.ClientApp.TxnDesc nextTxn(java.util.Random rnd) {
            if (allAccounts.isEmpty()) {
                return new paxos.client.ClientApp.TxnDesc("1", "2", 1);
            }

            boolean isReadOnly = rnd.nextDouble() < cfg.readOnlyRatio;
            if (isReadOnly) {
                int cust = sampleAccountGlobal(rnd);
                int acctId = checkingIdForAccount(cust);
                String sender = Integer.toString(acctId);
                return new paxos.client.ClientApp.TxnDesc(sender, "", 0);
            }

            boolean wantCross = rnd.nextDouble() < cfg.crossShardRatio;
            int[] clustersWithCust = clustersWithAccounts();
            if (clustersWithCust.length < 2) {
                wantCross = false;
            }

            if (wantCross) {
                int fromCluster = sampleClusterWithAccounts(rnd, clustersWithCust);
                int toCluster = fromCluster;
                if (clustersWithCust.length > 1) {
                    for (int attempt = 0; attempt < 5 && toCluster == fromCluster; attempt++) {
                        toCluster = clustersWithCust[rnd.nextInt(clustersWithCust.length)];
                    }
                    if (toCluster == fromCluster) {
                        wantCross = false;
                    }
                } else {
                    wantCross = false;
                }

                if (wantCross) {
                    int fromCust = sampleAccountInCluster(rnd, fromCluster);
                    int toCust = sampleAccountInCluster(rnd, toCluster);
                    if (accountsByCluster[toCluster].size() > 1) {
                        for (int attempt = 0; attempt < 5 && toCust == fromCust; attempt++) {
                            toCust = sampleAccountInCluster(rnd, toCluster);
                        }
                    }
                    int sAcc = checkingIdForAccount(fromCust);
                    int rAcc = checkingIdForAccount(toCust);
                    int amt = sampleAmount(rnd);
                    return new paxos.client.ClientApp.TxnDesc(Integer.toString(sAcc), Integer.toString(rAcc), amt);
                }
            }

            int[] clustersWithCust2 = clustersWithAccounts();
            int cluster = sampleClusterWithAccounts(rnd, clustersWithCust2);
            int cust1 = sampleAccountInCluster(rnd, cluster);
            int cust2 = sampleAccountInCluster(rnd, cluster);
            if (accountsByCluster[cluster].size() > 1) {
                for (int attempt = 0; attempt < 5 && cust2 == cust1; attempt++) {
                    cust2 = sampleAccountInCluster(rnd, cluster);
                }
            }
            int sAcc = checkingIdForAccount(cust1);
            int rAcc = checkingIdForAccount(cust2);
            int amt = sampleAmount(rnd);
            return new paxos.client.ClientApp.TxnDesc(Integer.toString(sAcc), Integer.toString(rAcc), amt);
        }

        private int[] clustersWithAccounts() {
            java.util.List<Integer> list = new java.util.ArrayList<>(3);
            for (int c = 1; c <= 3; c++) {
                if (cfg.targetCluster != 0 && c != cfg.targetCluster) continue;
                if (accountsByCluster[c] != null && !accountsByCluster[c].isEmpty()) {
                    list.add(c);
                }
            }
            int[] arr = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                arr[i] = list.get(i);
            }
            return arr;
        }

        private int sampleClusterWithAccounts(java.util.Random rnd, int[] clusters) {
            if (clusters == null || clusters.length == 0) {
                return 1;
            }
            return clusters[rnd.nextInt(clusters.length)];
        }

        private int sampleAccountGlobal(java.util.Random rnd) {
            boolean useHot = cfg.skew > 0.0 && !hotAccounts.isEmpty() && rnd.nextDouble() < cfg.skew;
            java.util.List<Integer> base = useHot ? hotAccounts : allAccounts;
            return base.get(rnd.nextInt(base.size()));
        }

        private int sampleAccountInCluster(java.util.Random rnd, int clusterId) {
            java.util.List<Integer> hotList = (hotAccountByCluster != null && clusterId >= 1 && clusterId < hotAccountByCluster.length)
                    ? hotAccountByCluster[clusterId]
                    : java.util.Collections.emptyList();
            java.util.List<Integer> allList = (accountsByCluster != null && clusterId >= 1 && clusterId < accountsByCluster.length)
                    ? accountsByCluster[clusterId]
                    : java.util.Collections.emptyList();
            boolean useHot = cfg.skew > 0.0 && !hotList.isEmpty() && rnd.nextDouble() < cfg.skew;
            java.util.List<Integer> baseList;
            if (useHot) {
                baseList = hotList;
            } else {
                baseList = allList.isEmpty() ? hotList : allList;
            }
            if (baseList.isEmpty()) {
                return sampleAccountGlobal(rnd);
            }
            return baseList.get(rnd.nextInt(baseList.size()));
        }

        private static int checkingIdForAccount(int customerId) {
            return 2 * customerId - 1;
        }

        @SuppressWarnings("unused")
        private static int savingsIdForAccount(int customerId) {
            return 2 * customerId;
        }

        private int sampleAmount(java.util.Random rnd) {
            return 1 + rnd.nextInt(5);
        }
    }

    private void runSmallBankBenchmark(paxos.client.ClientApp.SmallBankConfig cfg) throws InterruptedException {
        if (cfg == null || cfg.totalTx <= 0L) {
            System.out.println("SmallBank benchmark: no transactions to run.");
            return;
        }

        // Reset reshard state for fresh benchmark run (independent of CSV reshard)
        reshardPerformer.resetForBenchmark();

        // Auto-enable aggressive benchmark mode when 100% intra-shard (no cross-shard)
        boolean autoAggressive = (cfg.crossShardRatio <= 0.0);
        if (autoAggressive && !BENCHMARK_AGGRESSIVE_MODE) {
            //System.out.println("SmallBank: 100% intra-shard workload detected, enabling aggressive benchmark mode.");
        }

        boolean prevBenchmarkMode = benchmarkModeActive;
        boolean prevAutoAggressive = benchmarkAutoAggressive;
        benchmarkModeActive = true;
        benchmarkAutoAggressive = autoAggressive;
        try {
            resetPerformance();
            resetClientStateForNewSet();



            resetViewHistoryForCurrentSet();
            cluster.resumeAfterSetAll();
            cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);

            paxos.client.ClientApp.SmallBankWorkload workload = new paxos.client.ClientApp.SmallBankWorkload(cluster, cfg);

            java.util.List<Thread> threads = new java.util.ArrayList<>();
            long basePerClient = cfg.totalTx / cfg.numClients;
            long remainder = cfg.totalTx % cfg.numClients;

            for (int i = 0; i < cfg.numClients; i++) {
                long txCount = basePerClient + (i < remainder ? 1 : 0);
                final String clientId = "bench-" + (i + 1);
                Thread th = new Thread(() -> {
                    java.util.Random rnd = new java.util.Random(cfg.seed ^ (clientId.hashCode() * 31L));
                    for (long n = 0; n < txCount; n++) {
                        paxos.client.ClientApp.TxnDesc spec = workload.nextTxn(rnd);
                        if (spec == null) {
                            continue;
                        }
                        reshardPerformer.noteTxn(spec.sender, spec.receiver, spec.amount);
                        submit(clientId, spec.sender, spec.receiver, spec.amount);
                    }
                }, "smallbank-client-" + (i + 1));
                th.start();
                threads.add(th);
            }

            for (Thread th : threads) {
                try {
                    th.join();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }

            cluster.waitForCompletion(CLUSTER_SYNC_TIMEOUT);
            printPerformance();
        } finally {
            benchmarkModeActive = prevBenchmarkMode;
            benchmarkAutoAggressive = prevAutoAggressive;
        }
    }

    private void runSmallBankBenchmarkInteractive(paxos.client.ClientApp.SmallBankConfig initialCfg) throws InterruptedException {
        paxos.client.ClientApp.SmallBankConfig cfg = initialCfg;
        printSmallBankWorkloadInfo(cfg);
        while (true) {
            System.out.print("Benchmark SmallBank [enter=run, db [n], balance <id>, log [n], audit [n], status <seq> [n], status, view [n], reshard, performance [reset], benchinfo, help, exit]: ");
            final String line;
            try {
                line = STDIN.nextLine();
            } catch (NoSuchElementException ignored) {
                return;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.equalsIgnoreCase("run")
                    || trimmed.equalsIgnoreCase("start") || trimmed.equalsIgnoreCase("go")) {
                runSmallBankBenchmark(cfg);
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            String cmd = parts[0].toLowerCase(Locale.ROOT);
            if (cmd.equals("exit") || cmd.equals("quit")) {
                return;
            }
            switch (cmd) {
                case "db":
                    if (parts.length >= 2) {
                        try { printDB(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printDB(); }
                    } else { printDB(); }
                    break;
                case "balance":
                    if (parts.length >= 2) {
                        try {
                            int id = Integer.parseInt(parts[1]);
                            printBalance(id);
                        } catch (NumberFormatException nfe) {
                            System.out.println("balance command requires an integer id: balance <id>");
                        }
                    } else {
                        System.out.println("balance command requires an integer id: balance <id>");
                    }
                    break;
                case "log":
                    if (parts.length >= 2) {
                        try { printLogs(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printLogs(); }
                    } else { printLogs(); }
                    break;
                case "audit":
                    if (parts.length >= 2) {
                        try { printAudit(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printAudit(); }
                    } else { printAudit(); }
                    break;
                case "status":
                    if (parts.length == 1 || parts[1].equalsIgnoreCase("summary")) {
                        printStatusSummary();
                    } else {
                        try {
                            int seq = Integer.parseInt(parts[1]);
                            if (parts.length >= 3) {
                                try { printStatus(seq, Integer.parseInt(parts[2])); }
                                catch (NumberFormatException nfe) { printStatus(seq); }
                            } else { printStatus(seq); }
                        } catch (NumberFormatException nfe) {
                            System.out.println("Status command requires an integer sequence: status <seq>");
                        }
                    }
                    break;
                case "performance":
                case "perf":
                    if (parts.length >= 2 && parts[1].equalsIgnoreCase("reset")) {
                        resetPerformance();
                        System.out.println("Performance stats reset.");
                    } else {
                        printPerformance();
                    }
                    break;
                case "view":
                    if (parts.length >= 2) {
                        try { printView(Integer.parseInt(parts[1])); } catch (NumberFormatException nfe) { printView(); }
                    } else { printView(); }
                    break;
                case "reshard":
                    printReshard();
                    break;
                case "setcluster":
                    if (parts.length >= 2) {
                        try {
                            int tc = Integer.parseInt(parts[1]);
                            if (tc < 0 || tc > 3) {
                                System.out.println("setcluster requires 0 (all) or 1..3: setcluster <id>");
                                break;
                            }
                            cfg = new paxos.client.ClientApp.SmallBankConfig(
                                    cfg.totalTx,
                                    cfg.numClients,
                                    cfg.readOnlyRatio,
                                    cfg.crossShardRatio,
                                    cfg.skew,
                                    cfg.hotsetFraction,
                                    cfg.seed,
                                    tc);
                            System.out.println("Updated benchmark targetCluster to " + (tc == 0 ? "all" : ("C" + tc)) + ".");
                            printSmallBankWorkloadInfo(cfg);
                        } catch (NumberFormatException nfe) {
                            System.out.println("setcluster requires 0 (all) or 1..3: setcluster <id>");
                        }
                    } else {
                        System.out.println("Usage: setcluster <id> where id is 0 (all) or 1..3");
                    }
                    break;
                case "benchinfo":
                case "workload":
                case "config":
                    printSmallBankWorkloadInfo(cfg);
                    break;
                case "help":
                    System.out.println("Commands: run, db [n], balance <id>, log [n], audit [n], status <seq> [n], status (summary), view [n], reshard, performance [reset], setcluster <id>, benchinfo, help, exit");
                    break;
                default:
                    System.out.println("Unknown command. Type 'help' for options or press Enter to run the benchmark.");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.exit(2);
        }

        String initialLeader = System.getProperty("leaderTarget", "localhost:50051");
        List<String> peers = new ArrayList<>();
        System.getProperties().forEach((k, v) -> {
            String ks = String.valueOf(k);
            if (ks.startsWith("peer.")) peers.add(String.valueOf(v));
        });

        if (args.length >= 1 && "benchmark".equalsIgnoreCase(args[0])) {
            if (args.length < 7) {
                System.out.println("Usage: benchmark smallbank <numTx> <numClients> <readOnlyPct> <crossShardPct> <skew> [seed] [targetCluster]");
                System.exit(2);
            }
            String mode = args[1];
            if (!"smallbank".equalsIgnoreCase(mode)) {
                System.out.println("Currently only 'smallbank' benchmark is supported.");
                System.exit(2);
            }
            long numTx;
            int numClients;
            try {
                numTx = Long.parseLong(args[2]);
                numClients = Integer.parseInt(args[3]);
            } catch (NumberFormatException nfe) {
                System.out.println("Invalid numTx or numClients for benchmark.");
                System.exit(2);
                return;
            }
            double readOnlyRatio = parseRatioArg(args[4]);
            double crossShardRatio = parseRatioArg(args[5]);
            double skew = parseRatioArg(args[6]);
            long seed = (args.length >= 8) ? Long.parseLong(args[7]) : System.currentTimeMillis();
            int targetCluster = 0;
            if (args.length >= 9) {
                try {
                    targetCluster = Integer.parseInt(args[8]);
                } catch (NumberFormatException ignored) {
                    targetCluster = 0;
                }
            }

            paxos.client.ClientApp.SmallBankConfig cfg = new paxos.client.ClientApp.SmallBankConfig(
                    numTx,
                    numClients,
                    readOnlyRatio,
                    crossShardRatio,
                    skew,
                    0.01,
                    seed,
                    targetCluster);

            try (paxos.client.ClientApp app = new paxos.client.ClientApp(initialLeader, peers)) {
                app.runSmallBankBenchmarkInteractive(cfg);
            }
            return;
        }

        try (paxos.client.ClientApp app = new paxos.client.ClientApp(initialLeader, peers)) {
            Path csv = Paths.get(args[0]);
            app.runCsvConcurrent(csv);
            app.keepConsoleAlive();
        }
    }
}
