package paxos.client;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import paxos.proto.NodeServiceGrpc;
import paxos.proto.PaxosProto;

final class ReshardPerformer {

    private static final int MIN_ACCOUNT_ID = 1;
    private static final int MAX_ACCOUNT_ID = 9000;
    private static final int CLUSTER_COUNT = 3;

    private final ClusterCoordinator cluster;
    private final long reshardWaitTimeout;
    private final CopyOnWriteArrayList<ObservedTxn> observedTxns = new CopyOnWriteArrayList<>();
    private final ShardStruct shardStruct = new ShardStruct();
    private final Path layoutFile;
    private volatile int[] activeClusterMapping;

    ReshardPerformer(ClusterCoordinator cluster, long reshardWaitTimeout) {
        this.cluster = cluster;
        long base = Math.max(0L, reshardWaitTimeout);
        long reshardWait = Long.getLong("reshardWait", 1000L);
        this.reshardWaitTimeout = Math.max(base, reshardWait);

        boolean enablePersistence = Boolean.parseBoolean(System.getProperty("persistReshard", "false"));
        Path lf = null;
        if (enablePersistence) {
            String fileProp = System.getProperty("reshardLayoutFile");
            if (fileProp == null || fileProp.isBlank()) {
                String dataDir = System.getProperty("dataDir", "data");
                lf = Paths.get(dataDir, "reshard-layout.csv");
            } else {
                lf = Paths.get(fileProp);
            }
        }
        this.layoutFile = lf;

        if (this.layoutFile != null) {
            loadMappingFromFile();
        }
    }

    void noteTxn(String sender, String receiver, int amount) {
        if (sender == null || receiver == null) {
            return;
        }
        String s = sender.trim();
        String r = receiver.trim();
        if (s.isEmpty() || r.isEmpty()) {
            return;
        }
        if (amount <= 0) {
            return;
        }
        observedTxns.add(new ObservedTxn(s, r, amount));
    }

    /**
     * Reset observed transactions and cluster mapping for a fresh benchmark run.
     * This ensures benchmark reshard is independent of CSV reshard.
     */
    void resetForBenchmark() {
        observedTxns.clear();
        activeClusterMapping = null;
        // Reset to default cluster mapping based on ID ranges
        // Do not load from file - benchmark should start fresh
    }

    void runReshardAndPrint() {
        List<ObservedTxn> snapshot = snapshotTxns();
        if (snapshot.isEmpty()) {
            System.out.println("PrintReshard: no transactions observed yet; nothing to move.");
            return;
        }

        ReshardSketch sketch = computeReshardSketch(snapshot);
        if (sketch.moves.isEmpty()) {
            System.out.println("PrintReshard: current shard layout already consistent with observed workload; no records moved.");
            return;
        }

        boolean applied = applySketchToCluster(sketch);
        if (applied) {
            for (MoveDescriptor mv : sketch.moves) {
                System.out.printf("(%d, c%d, c%d)%n", mv.accountId, mv.fromCluster, mv.toCluster);
            }
            activeClusterMapping = sketch.clusterByAccount;
            persistMappingToFile(activeClusterMapping);
            cluster.applyShardLayout(sketch.clusterByAccount);
            observedTxns.clear();
        } else {
            System.out.println("PrintReshard: resharding plan not applied; logical layout unchanged.");
        }
    }

    private void loadMappingFromFile() {
        if (layoutFile == null) return;
        try {
            if (!Files.exists(layoutFile)) return;
            List<String> lines = Files.readAllLines(layoutFile, StandardCharsets.UTF_8);
            if (lines == null || lines.isEmpty()) return;

            int[] mapping = new int[MAX_ACCOUNT_ID + 1];
            int count = 0;

            for (String raw : lines) {
                if (raw == null) continue;
                String line = raw.trim();
                if (line.isEmpty()) continue;
                if (line.startsWith("#")) continue;
                String[] parts = line.split(",");
                if (parts.length < 2) continue;
                int id;
                int clusterId;
                try {
                    id = Integer.parseInt(parts[0].trim());
                    clusterId = Integer.parseInt(parts[1].trim());
                } catch (NumberFormatException nfe) {
                    continue;
                }
                if (!inRange(id)) continue;
                if (clusterId < 0 || clusterId > CLUSTER_COUNT) continue;
                mapping[id] = clusterId;
                count++;
            }

            if (count == 0) return;
            this.activeClusterMapping = mapping;
        } catch (Exception ignore) {

        }
    }

    private void persistMappingToFile(int[] mapping) {
        if (layoutFile == null || mapping == null) return;
        try {
            Path dir = layoutFile.toAbsolutePath().getParent();
            if (dir != null && !Files.exists(dir)) {
                Files.createDirectories(dir);
            }

            Path tmp = layoutFile.resolveSibling(layoutFile.getFileName().toString() + ".tmp");
            try (java.io.BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                w.write("# accountId,clusterId\n");
                for (int id = MIN_ACCOUNT_ID; id <= MAX_ACCOUNT_ID; id++) {
                    int c = (id < mapping.length ? mapping[id] : 0);
                    if (c < 0 || c > CLUSTER_COUNT) c = 0;
                    w.write(Integer.toString(id));
                    w.write(",");
                    w.write(Integer.toString(c));
                    w.newLine();
                }
            }

            try {
                Files.move(tmp, layoutFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {

                Files.move(tmp, layoutFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception ignore) {

        }
    }

    private List<ObservedTxn> snapshotTxns() {
        if (observedTxns.isEmpty()) {
            return Collections.emptyList();
        }
        java.util.List<ObservedTxn> snapshot = new ArrayList<>(observedTxns);
        Integer limitBoxed = Integer.getInteger("reshardHistorySize", 10000);
        int limit = (limitBoxed == null ? 10000 : limitBoxed);
        if (limit > 0 && snapshot.size() > limit) {
            int start = snapshot.size() - limit;
            snapshot = new ArrayList<>(snapshot.subList(start, snapshot.size()));
        }
        return snapshot;
    }

    private ReshardSketch computeReshardSketch(List<ObservedTxn> txns) {
        int[] clusterByAccount = new int[MAX_ACCOUNT_ID + 1];
        int[] baseline = activeClusterMapping;
        if (baseline != null && baseline.length == clusterByAccount.length) {
            System.arraycopy(baseline, 0, clusterByAccount, 0, clusterByAccount.length);
        } else {
            for (int id = MIN_ACCOUNT_ID; id <= MAX_ACCOUNT_ID; id++) {
                int c = shardStruct.clusterForAccount(Integer.toString(id));
                clusterByAccount[id] = c;
            }
        }

        Map<Integer, Map<Integer, Integer>> adjacency = new HashMap<>();
        for (ObservedTxn ot : txns) {
            int s = parseAccountId(ot.senderId);
            int r = parseAccountId(ot.receiverId);
            if (!inRange(s) || !inRange(r) || s == r) {
                continue;
            }
            adjacency.computeIfAbsent(s, k -> new HashMap<>())
                    .merge(r, 1, Integer::sum);
            adjacency.computeIfAbsent(r, k -> new HashMap<>())
                    .merge(s, 1, Integer::sum);
        }

        if (adjacency.isEmpty()) {
            return new ReshardSketch(clusterByAccount, Collections.emptyList());
        }

        List<MoveDescriptor> candidates = new ArrayList<>();
        for (Integer accountId : adjacency.keySet()) {
            int id = accountId;
            if (!inRange(id)) {
                continue;
            }
            int curCluster = clusterByAccount[id];
            if (curCluster < 1 || curCluster > CLUSTER_COUNT) {
                continue;
            }
            long baseCost = computeCrossCost(id, curCluster, clusterByAccount, adjacency);
            for (int target = 1; target <= CLUSTER_COUNT; target++) {
                if (target == curCluster) {
                    continue;
                }
                long altCost = computeCrossCost(id, target, clusterByAccount, adjacency);
                long gain = baseCost - altCost;
                if (gain > 0L) {
                    candidates.add(new MoveDescriptor(id, curCluster, target, gain));
                }
            }
        }

        if (candidates.isEmpty()) {
            return new ReshardSketch(clusterByAccount, Collections.emptyList());
        }

        candidates.sort((a, b) -> {
            int cmp = Long.compare(b.gain, a.gain);
            if (cmp != 0) return cmp;
            return Integer.compare(a.accountId, b.accountId);
        });

        int[] clusterSizes = new int[CLUSTER_COUNT + 1];
        int totalAccounts = 0;
        for (int id = MIN_ACCOUNT_ID; id <= MAX_ACCOUNT_ID; id++) {
            int c = clusterByAccount[id];
            if (c >= 1 && c <= CLUSTER_COUNT) {
                clusterSizes[c]++;
                totalAccounts++;
            }
        }
        int targetSize = (totalAccounts <= 0 ? 0 : (totalAccounts + CLUSTER_COUNT - 1) / CLUSTER_COUNT);
        int slack = Math.max(1, totalAccounts / 100);
        List<MoveDescriptor> accepted = new ArrayList<>();

        for (MoveDescriptor cand : candidates) {
            int id = cand.accountId;
            int current = clusterByAccount[id];
            if (current != cand.fromCluster || current == cand.toCluster) {
                continue;
            }
            int from = cand.fromCluster;
            int to = cand.toCluster;
            if (from < 1 || from > CLUSTER_COUNT || to < 1 || to > CLUSTER_COUNT) {
                continue;
            }
            int newFromSize = clusterSizes[from] - 1;
            int newToSize = clusterSizes[to] + 1;
            if (newFromSize < targetSize - slack) {
                continue;
            }
            if (newToSize > targetSize + slack) {
                continue;
            }
            clusterByAccount[id] = to;
            clusterSizes[from] = newFromSize;
            clusterSizes[to] = newToSize;
            accepted.add(new MoveDescriptor(id, from, to, cand.gain));
        }

        return new ReshardSketch(clusterByAccount, accepted);
    }

    private boolean applySketchToCluster(ReshardSketch sketch) {
        if (sketch == null || sketch.moves.isEmpty()) {
            return false;
        }

        // Recover all failed nodes before applying reshard to ensure all nodes receive the deltas
        cluster.recoverAllNodes();
        
        cluster.waitForCompletion(reshardWaitTimeout);

        java.util.LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = cluster.allNodesWithIds();
        if (nodes.isEmpty()) {
            System.out.println("PrintReshard: no reachable nodes for applying shard delta.");
            return false;
        }

        Map<Integer, List<NodeServiceGrpc.NodeServiceBlockingStub>> stubsByCluster = new HashMap<>();
        Map<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodeIds = new HashMap<>();
        for (var entry : nodes.entrySet()) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
            Integer idBoxed = entry.getValue();
            int nodeId = (idBoxed == null ? 0 : idBoxed);
            nodeIds.put(stub, nodeId);
            int clusterId = cluster.clusterIdForNodeId(nodeId);
            if (clusterId < 1 || clusterId > CLUSTER_COUNT) {
                continue;
            }
            stubsByCluster.computeIfAbsent(clusterId, k -> new ArrayList<>()).add(stub);
        }

        for (int c = 1; c <= CLUSTER_COUNT; c++) {
            List<NodeServiceGrpc.NodeServiceBlockingStub> list = stubsByCluster.get(c);
            if (list == null || list.isEmpty()) {
                System.out.printf("PrintReshard: cluster c%d has no reachable nodes; aborting apply.%n", c);
                return false;
            }
            // Apply reshard to all reachable nodes - nodes that are down will miss it
            // but we proceed anyway as requested
        }

        Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> readerByCluster = new HashMap<>();
        for (int c = 1; c <= CLUSTER_COUNT; c++) {
            List<NodeServiceGrpc.NodeServiceBlockingStub> list = stubsByCluster.get(c);
            NodeServiceGrpc.NodeServiceBlockingStub best = null;
            int bestId = Integer.MAX_VALUE;
            for (NodeServiceGrpc.NodeServiceBlockingStub stub : list) {
                int nid = nodeIds.getOrDefault(stub, 0);
                if (nid > 0 && nid < bestId) {
                    bestId = nid;
                    best = stub;
                }
            }
            if (best == null) {
                best = list.get(0);
            }
            readerByCluster.put(c, best);
        }

        Map<Integer, Map<String,Integer>> balancesByCluster = new HashMap<>();
        PaxosProto.Empty empty = PaxosProto.Empty.getDefaultInstance();
        for (int c = 1; c <= CLUSTER_COUNT; c++) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = readerByCluster.get(c);
            Map<String,Integer> map = new HashMap<>();
            try {
                PaxosProto.DBDump dump = ClientApp.timeout(stub).getDBOnDisk(empty);
                for (PaxosProto.KV kv : dump.getBalancesList()) {
                    String key = kv.getKey();
                    if (key == null) {
                        continue;
                    }
                    String trimmed = key.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    map.put(trimmed, kv.getValue());
                }
            } catch (Exception ex) {
                System.out.printf("PrintReshard: failed to snapshot cluster c%d: %s%n", c, ex.getMessage());
                return false;
            }
            balancesByCluster.put(c, map);
        }

        Map<NodeServiceGrpc.NodeServiceBlockingStub, PaxosProto.ShardDelta.Builder> deltaByStub = new HashMap<>();

        for (MoveDescriptor mv : sketch.moves) {
            String key = Integer.toString(mv.accountId);
            Map<String,Integer> srcMap = balancesByCluster.get(mv.fromCluster);
            Integer bal = (srcMap != null ? srcMap.get(key) : null);
            if (bal == null) {
                System.out.printf("PrintReshard: source cluster c%d missing account %s; aborting reshard.%n",
                        mv.fromCluster, key);
                return false;
            }

            List<NodeServiceGrpc.NodeServiceBlockingStub> fromStubs = stubsByCluster.get(mv.fromCluster);
            if (fromStubs != null) {
                for (NodeServiceGrpc.NodeServiceBlockingStub stub : fromStubs) {
                    PaxosProto.ShardDelta.Builder b =
                            deltaByStub.computeIfAbsent(stub, s -> PaxosProto.ShardDelta.newBuilder());
                    b.addDrop(key);
                }
            }

            List<NodeServiceGrpc.NodeServiceBlockingStub> toStubs = stubsByCluster.get(mv.toCluster);
            if (toStubs != null) {
                for (NodeServiceGrpc.NodeServiceBlockingStub stub : toStubs) {
                    PaxosProto.ShardDelta.Builder b =
                            deltaByStub.computeIfAbsent(stub, s -> PaxosProto.ShardDelta.newBuilder());
                    b.addAssign(PaxosProto.KV.newBuilder().setKey(key).setValue(bal).build());
                }
            }
        }

        if (deltaByStub.isEmpty()) {
            System.out.println("PrintReshard: computed moves but produced no concrete deltas; aborting apply.");
            return false;
        }

        boolean ok = true;
        for (var entry : deltaByStub.entrySet()) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
            PaxosProto.ShardDelta delta = entry.getValue().build();
            try {
                ClientApp.timeout(stub).applyShardDelta(delta);
            } catch (Exception ex) {
                ok = false;
                System.out.printf("PrintReshard: ApplyShardDelta failed toward a node: %s%n", ex.getMessage());
            }
        }

        cluster.waitForCompletion(reshardWaitTimeout);
        return ok;
    }

    private long computeCrossCost(int accountId,
                                  int candidateCluster,
                                  int[] clusterByAccount,
                                  Map<Integer, Map<Integer, Integer>> adjacency) {
        Map<Integer, Integer> neighbors = adjacency.get(accountId);
        if (neighbors == null || neighbors.isEmpty()) {
            return 0L;
        }
        long cost = 0L;
        for (Map.Entry<Integer, Integer> e : neighbors.entrySet()) {
            int neighborId = e.getKey();
            int weight = e.getValue();
            if (!inRange(neighborId)) {
                continue;
            }
            int neighborCluster = clusterByAccount[neighborId];
            if (neighborCluster != 0 && neighborCluster != candidateCluster) {
                cost += weight;
            }
        }
        return cost;
    }

    private int parseAccountId(String raw) {
        if (raw == null) {
            return -1;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (Exception e) {
            return -1;
        }
    }

    private boolean inRange(int id) {
        return id >= MIN_ACCOUNT_ID && id <= MAX_ACCOUNT_ID;
    }

    private static final class ObservedTxn {
        final String senderId;
        final String receiverId;
        final int amount;

        ObservedTxn(String senderId, String receiverId, int amount) {
            this.senderId = senderId;
            this.receiverId = receiverId;
            this.amount = amount;
        }
    }

    private static final class MoveDescriptor {
        final int accountId;
        final int fromCluster;
        final int toCluster;
        final long gain;

        MoveDescriptor(int accountId, int fromCluster, int toCluster, long gain) {
            this.accountId = accountId;
            this.fromCluster = fromCluster;
            this.toCluster = toCluster;
            this.gain = gain;
        }
    }

    private static final class ReshardSketch {
        final int[] clusterByAccount;
        final List<MoveDescriptor> moves;

        ReshardSketch(int[] clusterByAccount, List<MoveDescriptor> moves) {
            this.clusterByAccount = clusterByAccount;
            this.moves = moves;
        }
    }
}
