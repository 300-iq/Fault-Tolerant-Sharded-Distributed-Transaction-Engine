package paxos.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import paxos.proto.NodeServiceGrpc;
import paxos.proto.PaxosProto;
import paxos.proto.PaxosProto.Empty;
import paxos.proto.PaxosProto.LivePeers;
import paxos.proto.PaxosProto.LogDump;
import paxos.proto.PaxosProto.NewViewMsg;
import paxos.proto.PaxosProto.AcceptMsg;
import paxos.proto.PaxosProto.ClientRequest;
import paxos.proto.PaxosProto.StatusQuery;
import paxos.proto.PaxosProto.StatusReply;
import paxos.proto.PaxosProto.ViewDump;
import paxos.proto.PaxosProto.AuditDump;
import paxos.utils.PrintHelper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class ClusterCoordinator implements AutoCloseable {

    private static final int INITIAL_BALANCE = 10;

    private final NodeServiceGrpc.NodeServiceBlockingStub bootstrap;
    private final List<NodeServiceGrpc.NodeServiceBlockingStub> allStubs;
    private final ConcurrentHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> stubNodeIds = new ConcurrentHashMap<>();
    private final AtomicInteger learntLeaderId = new AtomicInteger(0);
    private final AtomicInteger[] learntLeaderIdsByCluster = new AtomicInteger[] {
            new AtomicInteger(0),
            new AtomicInteger(0),
            new AtomicInteger(0)
    };
    private volatile Set<Integer> liveNodeIds = Collections.emptySet();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final ShardStruct shardStruct = new ShardStruct();

    ClusterCoordinator(String bootstrapTarget, java.util.Collection<String> peerTargets) {
        LinkedHashSet<String> targets = new LinkedHashSet<>();
        targets.add(bootstrapTarget);
        if (peerTargets != null) targets.addAll(peerTargets);
        Map<String, Integer> configured = collectConfiguredNodeIds();

        List<NodeServiceGrpc.NodeServiceBlockingStub> tmp = new ArrayList<>();
        targets.forEach(t ->  {
            ManagedChannel ch = ManagedChannelBuilder.forTarget(t).usePlaintext().build();
            channels.add(ch);
            NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(ch);
            tmp.add(stub);
            stubNodeIds.put(stub, configured.getOrDefault(t, 0));
        });
        if (tmp.isEmpty()) {
            throw new IllegalArgumentException("No bootstrap target configured");
        }
        this.bootstrap = tmp.get(0);
        this.allStubs = Collections.unmodifiableList(tmp);
    }

    void applyShardLayout(int[] clusterByAccount) {
        shardStruct.applyShardLayout(clusterByAccount);
    }

    @Override public void close() {
        channels.forEach(ch -> {
            try {
                ch.shutdownNow();
            } catch (Exception ignored) {

            }
        });
    }

    List<NodeServiceGrpc.NodeServiceBlockingStub> constructNodeOrder() {
        LinkedHashSet<NodeServiceGrpc.NodeServiceBlockingStub> order = new LinkedHashSet<>();
        int leaderId = learntLeaderId.get();
        NodeServiceGrpc.NodeServiceBlockingStub leaderStub = getStubFromNodeId(leaderId);
        if (leaderStub != null && isStubAllowed(leaderStub)) {
            order.add(leaderStub);
        }
        if (isStubAllowed(bootstrap)) {
            order.add(bootstrap);
        }
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : allStubs) {
            if (isStubAllowed(stub)) {
                order.add(stub);
            }
        }
        return new ArrayList<>(order);
    }

    void rememberLeader(paxos.proto.PaxosProto.Reply rep) {
        if (rep == null || !rep.hasLeaderBallot()) return;
        int leaderId = rep.getLeaderBallot().getNodeId();
        if (leaderId <= 0) return;
        if (getStubFromNodeId(leaderId) == null) return;

        learntLeaderId.updateAndGet(prev -> prev == leaderId ? prev : leaderId);

        int cluster = clusterForNodeId(leaderId);
        if (cluster >= 1 && cluster <= 3) {
            AtomicInteger slot = learntLeaderIdsByCluster[cluster - 1];
            slot.updateAndGet(prev -> prev == leaderId ? prev : leaderId);
        }
    }

    void setLiveNodes(List<String> liveNodsList) {
        if (liveNodsList == null || liveNodsList.isEmpty()) {
            liveNodeIds = Collections.emptySet();
            learntLeaderId.set(0);
        } else {
            LinkedHashSet<Integer> next = new LinkedHashSet<>();
            for (String entry : liveNodsList) {
                int nodeId = parseNodeIdString(entry);
                if (nodeId > 0) next.add(nodeId);
            }
            liveNodeIds = Collections.unmodifiableSet(next);
            if (!next.contains(learntLeaderId.get())) {
                learntLeaderId.set(0);
            }
        }
        syncLivePeers();
    }

    /**
     * Recover all nodes by setting all 9 nodes as live.
     * Called before reshard to ensure all nodes receive the reshard deltas.
     */
    void recoverAllNodes() {
        List<String> allNodes = Arrays.asList("n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9");
        setLiveNodes(allNodes);
        //System.out.println("Recovered all nodes before reshard: " + allNodes);
    }

    void resetViewHistoryForCurrentSet() {
        Empty empty = Empty.getDefaultInstance();
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : allStubs) {
            Integer nodeId = stubNodeIds.getOrDefault(stub, 0);
            try {
                log("resetViewHistoryForCurrentSet: sending resetView to nodeId=%d", nodeId);
                ClientApp.timeout(stub).resetView(empty);
                log("resetViewHistoryForCurrentSet: resetView OK on nodeId=%d", nodeId);
            } catch (Exception e) {
                log("resetViewHistoryForCurrentSet: resetView FAILED on nodeId=%d: %s", nodeId, e.toString());
            }
        }
    }

    void pauseTimersAll() {
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : allStubs) {
            Integer nodeId = stubNodeIds.getOrDefault(stub, 0);
            if (!liveNodeIds.isEmpty() && (nodeId == 0 || !liveNodeIds.contains(nodeId))) continue;
            try { ClientApp.timeout(stub).pauseTimers(Empty.getDefaultInstance()); } catch (Exception ignored) {}
        }
    }


    void resumeAfterSetAll() {
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : allStubs) {
            Integer nodeId = stubNodeIds.getOrDefault(stub, 0);
            if (!liveNodeIds.isEmpty() && (nodeId == 0 || !liveNodeIds.contains(nodeId))) continue;
            try { ClientApp.timeout(stub).resumeAfterSet(Empty.getDefaultInstance()); } catch (Exception ignored) {}
        }
    }

    void suspendLeaderForSet() {
        Empty empty = Empty.getDefaultInstance();
        LinkedHashSet<NodeServiceGrpc.NodeServiceBlockingStub> targets = new LinkedHashSet<>();

        NodeServiceGrpc.NodeServiceBlockingStub leader = getStubFromNodeId(learntLeaderId.get());
        if (leader != null && isStubAllowed(leader)) {
            targets.add(leader);
        }
        targets.addAll(constructNodeOrder());

        boolean delivered = false;


        for (NodeServiceGrpc.NodeServiceBlockingStub stub : targets) {
            try {
                ClientApp.timeout(stub).suspendForSet(empty);
                delivered = true;
            } catch (Exception ignored) { }
        }

        int failedLeaderId = learntLeaderId.get();
        if (failedLeaderId > 0) {

            java.util.LinkedHashSet<Integer> nextLive = new java.util.LinkedHashSet<>();
            if (liveNodeIds.isEmpty()) {

                stubNodeIds.forEach((stub, nodeId) -> {
                    Integer id = nodeId;
                    if (id == null || id == 0) {
                        int resolved = resolveNodeId(stub);
                        if (resolved != 0) id = resolved;
                    }
                    if (id != null && id != 0 && id != failedLeaderId) {
                        nextLive.add(id);
                    }
                });
            }
            else {
                liveNodeIds.forEach(id -> {
                    if (id != failedLeaderId) nextLive.add(id);
                });

            }

            liveNodeIds = java.util.Collections.unmodifiableSet(nextLive);
            PaxosProto.LivePeers.Builder lb = PaxosProto.LivePeers.newBuilder();
            nextLive.forEach(id -> lb.addNodeIds(id));
            PaxosProto.LivePeers payload = lb.build();
            allStubs.forEach(stub -> {
                try {
                    ClientApp.timeout(stub).setLivePeers(payload);
                } catch (Exception ignored) {
                }
            });
        }

        if (!delivered) {
            log("LF command: unable to reach any eligible node");
        }
    }

    LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> reachableNodes() {
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = new LinkedHashMap<>();
        StatusQuery probe = StatusQuery.newBuilder().setSeq(0).build();
        allStubs.forEach(stub -> {
            Integer preset = stubNodeIds.get(stub);
            if (!liveNodeIds.isEmpty() && (preset == null || preset == 0 || !liveNodeIds.contains(preset))) {
                return;
            }
            try {
                StatusReply info = ClientApp.timeout(stub).getStatus(probe);
                stubNodeIds.putIfAbsent(stub, info.getNodeId());
                if (nodeAllowed(info.getNodeId())) {
                    nodes.put(stub, info.getNodeId());
                }
            } catch (Exception ignored) { }
        });
        return nodes;
    }

    void printDB() { printDB(null); }

    void printDB(Integer nodeIdOpt) {
        Empty empty = Empty.getDefaultInstance();
        if (nodeIdOpt != null && nodeIdOpt > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getStubFromNodeId(nodeIdOpt);
            int nodeId = nodeIdOpt;
            if (stub == null) {
                System.out.printf("DB node %d: error%n", nodeId);
                return;
            }
            try {
                paxos.proto.PaxosProto.DBDump db = ClientApp.timeout(stub).getModifiedDB(empty);
                System.out.print("DB node " + nodeId + ": ");
                java.util.List<paxos.proto.PaxosProto.KV> list = new java.util.ArrayList<>(db.getBalancesList());
                list.sort((a, b) -> {
                    try {
                        int ka = Integer.parseInt(a.getKey());
                        int kb = Integer.parseInt(b.getKey());
                        return Integer.compare(ka, kb);
                    } catch (Exception ignore) {
                        return a.getKey().compareTo(b.getKey());
                    }
                });
                for (paxos.proto.PaxosProto.KV kv : list) {
                    System.out.print(kv.getKey() + "=" + kv.getValue() + "  ");
                }
                System.out.println();
            } catch (Exception e) {
                System.out.printf("DB node %d: error%n", nodeId);
            }
            return;
        }
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        java.util.List<java.util.Map.Entry<NodeServiceGrpc.NodeServiceBlockingStub, Integer>> ordered = new java.util.ArrayList<>(nodes.entrySet());
        ordered.sort(java.util.Comparator.comparingInt(e -> e.getValue() == null ? 0 : e.getValue()));
        for (var entry : ordered) {
            int nodeId = entry.getValue();
            try {
                paxos.proto.PaxosProto.DBDump db = ClientApp.timeout(entry.getKey()).getModifiedDB(empty);
                System.out.print("DB node " + nodeId + ": ");
                java.util.List<paxos.proto.PaxosProto.KV> list = new java.util.ArrayList<>(db.getBalancesList());
                list.sort((a, b) -> {
                    try {
                        int ka = Integer.parseInt(a.getKey());
                        int kb = Integer.parseInt(b.getKey());
                        return Integer.compare(ka, kb);
                    } catch (Exception ignore) {
                        return a.getKey().compareTo(b.getKey());
                    }
                });
                for (paxos.proto.PaxosProto.KV kv : list) {
                    System.out.print(kv.getKey() + "=" + kv.getValue() + "  ");
                }
                System.out.println();
            } catch (Exception e) {
                System.out.printf("DB node %d: error%n", nodeId);
            }
        }
    }

    void printBalance(int accountId) {
        int cluster = clusterForAccountId(Integer.toString(accountId));
        if (cluster == 0) {
            System.out.printf("PrintBalance(%d): invalid account id%n", accountId);
            return;
        }

        Empty empty = Empty.getDefaultInstance();
        boolean persistDBOnDisk = Boolean.parseBoolean(System.getProperty("persistDBOnDisk", "true"));

        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        Map<Integer, Integer> balances = new java.util.TreeMap<>();

        for (var entry : nodes.entrySet()) {
            int nodeId = entry.getValue();
            if (clusterForNodeId(nodeId) != cluster) continue;
            try {
                paxos.proto.PaxosProto.DBDump db = persistDBOnDisk
                        ? ClientApp.timeout(entry.getKey()).getDBOnDisk(empty)
                        : ClientApp.timeout(entry.getKey()).getDB(empty);
                Integer bal = null;
                String key = Integer.toString(accountId);
                for (paxos.proto.PaxosProto.KV kv : db.getBalancesList()) {
                    if (kv.getKey().equals(key)) {
                        bal = kv.getValue();
                        break;
                    }
                }
                if (bal != null) {
                    balances.put(nodeId, bal);
                } else {
                    balances.put(nodeId, null);
                }
            } catch (Exception e) {
                balances.put(nodeId, null);
            }
        }

        if (balances.isEmpty()) {
            System.out.printf("PrintBalance(%d): no nodes in cluster%n", accountId);
            return;
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<Integer, Integer> e : balances.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append("n").append(e.getKey()).append(" : ");
            Integer bal = e.getValue();
            if (bal == null) {
                sb.append("ERR");
            } else {
                sb.append(bal);
            }
        }
        System.out.println(sb.toString());
    }


    void waitForCompletion(long timeoutMs) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMs));
        Empty empty = Empty.getDefaultInstance();
        while (System.nanoTime() < deadline) {
            LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = reachableNodes();
            if (nodes.isEmpty()) return;


            for (var entry : nodes.entrySet()) {
                try { ClientApp.timeout(entry.getKey()).getDB(empty); } catch (Exception ignored) { }
            }


            java.util.Set<Integer> seqs = new java.util.TreeSet<>();
            for (var entry : nodes.entrySet()) {
                try {
                    LogDump dump = ClientApp.timeout(entry.getKey()).getLog(empty);
                    for (paxos.proto.PaxosProto.LogEntryDump led : dump.getEntriesList()) {
                        seqs.add(led.getSeq());
                    }
                } catch (Exception ignored) { }
            }
            if (seqs.isEmpty()) return;

            boolean allExecuted = true;
            for (int seq : seqs) {
                StatusQuery q = StatusQuery.newBuilder().setSeq(seq).build();
                for (var entry : nodes.entrySet()) {
                    try {
                        StatusReply sr = ClientApp.timeout(entry.getKey()).getStatus(q);
                        if (!sr.getStatus().equals(PaxosProto.TxnStatus.E)) { allExecuted = false; break; }
                    } catch (Exception e) {
                        allExecuted = false; break;
                    }
                }
                if (!allExecuted) break;
            }
            if (allExecuted) return;

            try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
        }
    }

    void printLogs() { printLogs(null); }

    void printLogs(Integer nodeIdOpt) {
        Empty empty = Empty.getDefaultInstance();
        if (nodeIdOpt != null && nodeIdOpt > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getStubFromNodeId(nodeIdOpt);
            int nodeId = nodeIdOpt;
            if (stub == null) {
                System.out.printf("Log node %d: error%n", nodeId);
                return;
            }
            try {
                LogDump dump = ClientApp.timeout(stub).getLog(empty);
                System.out.printf("Log node %d:%n", nodeId);
                if (dump.getEntriesCount() == 0) {
                    System.out.println("  <empty>");
                } else {
                    for (paxos.proto.PaxosProto.LogEntryDump led : dump.getEntriesList()) {
                        System.out.printf("  s=%d phase=%s tag=%s client=%s payload=%s%n",
                                led.getSeq(), led.getPhase(), led.getTwoPcTag(), led.getClientId(), led.getPayload());
                    }
                }
            } catch (Exception e) {
                System.out.printf("Log node %d: error%n", nodeId);
            }
            return;
        }
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        java.util.List<java.util.Map.Entry<NodeServiceGrpc.NodeServiceBlockingStub, Integer>> ordered = new java.util.ArrayList<>(nodes.entrySet());
        ordered.sort(java.util.Comparator.comparingInt(e -> e.getValue() == null ? 0 : e.getValue()));
        for (var entry : ordered) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
            int nodeId = entry.getValue();
            try {
                LogDump dump = ClientApp.timeout(stub).getLog(empty);
                System.out.printf("Log node %d:%n", nodeId);
                if (dump.getEntriesCount() == 0) {
                    System.out.println("  <empty>");
                } else {
                    for (paxos.proto.PaxosProto.LogEntryDump led : dump.getEntriesList()) {
                        System.out.printf("  s=%d phase=%s tag=%s client=%s payload=%s%n",
                                led.getSeq(), led.getPhase(), led.getTwoPcTag(), led.getClientId(), led.getPayload());
                    }
                }
            } catch (Exception e) {
                System.out.printf("Log node %d: error%n", nodeId);
            }
        }
    }

    void printAudit() { printAudit(null); }

    void printAudit(Integer nodeIdOpt) {
        Empty empty = Empty.getDefaultInstance();
        if (nodeIdOpt != null && nodeIdOpt > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getStubFromNodeId(nodeIdOpt);
            int nodeId = nodeIdOpt;
            if (stub == null) {
                System.out.printf("Audit node %d: error%n", nodeId);
                return;
            }
            try {
                AuditDump dump = ClientApp.timeout(stub).getAudit(empty);
                System.out.printf("Audit node %d:%n", nodeId);
                if (dump.getEntriesCount() == 0) {
                    System.out.println("  <empty>");
                } else {
                    dump.getEntriesList().forEach(a -> {
                        String ballot = (a.getBallotRound() >= 0 && a.getBallotNode() >= 0)
                                ? String.format("(%d,%d)", a.getBallotRound(), a.getBallotNode())
                                : "(?,?)";
                        boolean noop = a.getClientId().isEmpty()
                                && a.getReqTs() == 0
                                && a.getAmount() == 0;
                        String payload = noop
                                ? "noop"
                                : String.format("clientId=%s ts=%d %s->%s:%d",
                                        a.getClientId(), a.getReqTs(),
                                        a.getSender(), a.getReceiver(), a.getAmount());
                        String note = a.getNote().isEmpty() ? "" : (" note=" + a.getNote());
                        System.out.printf("  ts=%d dir=%s kind=%s peer=%d ballot=%s seq=%d %s%s%n",
                                a.getTs(), a.getDir(), a.getKind(), a.getPeer(),
                                ballot, a.getSeq(), payload, note);
                    });
                }
            } catch (Exception e) {
                System.out.printf("Audit node %d: error%n", nodeId);
            }
            return;
        }

        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        java.util.List<java.util.Map.Entry<NodeServiceGrpc.NodeServiceBlockingStub, Integer>> ordered = new java.util.ArrayList<>(nodes.entrySet());
        ordered.sort(java.util.Comparator.comparingInt(e -> e.getValue() == null ? 0 : e.getValue()));
        for (var entry : ordered) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
            int nodeId = entry.getValue();
            try {
                AuditDump dump = ClientApp.timeout(stub).getAudit(empty);
                System.out.printf("Audit node %d:%n", nodeId);
                if (dump.getEntriesCount() == 0) {
                    System.out.println("  <empty>");
                    continue;
                }
                dump.getEntriesList().forEach(a -> {
                    String ballot = (a.getBallotRound() >= 0 && a.getBallotNode() >= 0)
                            ? String.format("(%d,%d)", a.getBallotRound(), a.getBallotNode())
                            : "(?,?)";
                    boolean noop = a.getClientId().isEmpty()
                            && a.getReqTs() == 0
                            && a.getAmount() == 0;
                    String payload = noop
                            ? "noop"
                            : String.format("clientId=%s ts=%d %s->%s:%d",
                                    a.getClientId(), a.getReqTs(),
                                    a.getSender(), a.getReceiver(), a.getAmount());
                    String note = a.getNote().isEmpty() ? "" : (" note=" + a.getNote());
                    System.out.printf("  ts=%d dir=%s kind=%s peer=%d ballot=%s seq=%d %s%s%n",
                            a.getTs(), a.getDir(), a.getKind(), a.getPeer(),
                            ballot, a.getSeq(), payload, note);
                });
            } catch (Exception e) {
                System.out.printf("Audit node %d: error%n", nodeId);
            }
        }
    }

    void printStatus(int seq) { printStatus(seq, null); }

    void printStatus(int seq, Integer nodeIdOpt) {
        StatusQuery query = StatusQuery.newBuilder().setSeq(seq).build();
        if (nodeIdOpt != null && nodeIdOpt > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getStubFromNodeId(nodeIdOpt);
            int nodeId = nodeIdOpt;
            System.out.printf("Seq %d:", seq);
            if (stub == null) {
                System.out.printf(" n%d=ERR", nodeId);
            } else {
                try {
                    StatusReply sr = ClientApp.timeout(stub).getStatus(query);
                    System.out.printf(" n%d=%s", nodeId, sr.getStatus());
                } catch (Exception e) {
                    System.out.printf(" n%d=ERR", nodeId);
                }
            }
            System.out.println();
            return;
        }
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        java.util.List<java.util.Map.Entry<NodeServiceGrpc.NodeServiceBlockingStub, Integer>> ordered = new java.util.ArrayList<>(nodes.entrySet());
        ordered.sort(java.util.Comparator.comparingInt(e -> e.getValue() == null ? 0 : e.getValue()));
        System.out.printf("Seq %d:", seq);
        for (var entry : ordered) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
            int nodeId = entry.getValue();
            try {
                StatusReply sr = ClientApp.timeout(stub).getStatus(query);
                System.out.printf(" n%d=%s", nodeId, sr.getStatus());
            } catch (Exception e) {
                System.out.printf(" n%d=ERR", nodeId);
            }
        }
        System.out.println();
    }

    void printStatusSummary() {
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        java.util.List<java.util.Map.Entry<NodeServiceGrpc.NodeServiceBlockingStub, Integer>> ordered = new java.util.ArrayList<>(nodes.entrySet());
        ordered.sort(java.util.Comparator.comparingInt(e -> e.getValue() == null ? 0 : e.getValue()));
        Empty empty = Empty.getDefaultInstance();
        Set<Integer> seqs = new TreeSet<>();
        for (var entry : ordered) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
            try {
                LogDump dump = ClientApp.timeout(stub).getLog(empty);
                dump.getEntriesList().forEach(led -> seqs.add(led.getSeq()));
            } catch (Exception ignored) {
            }
        }
        if (seqs.isEmpty()) {
            System.out.println("Status: no log entries yet");
            return;
        }
        StatusQuery.Builder queryBuilder = StatusQuery.newBuilder();
        seqs.forEach(seq -> {
            System.out.printf("Seq %d:", seq);
            queryBuilder.setSeq(seq);
            StatusQuery query = queryBuilder.build();

            for (var entry : ordered) {
                NodeServiceGrpc.NodeServiceBlockingStub stub = entry.getKey();
                int nodeId = entry.getValue();
                try {
                    StatusReply sr = ClientApp.timeout(stub).getStatus(query);
                    System.out.printf(" n%d=%s", nodeId, sr.getStatus());
                } catch (Exception e) {
                    System.out.printf(" n%d=ERR", nodeId);
                }
            }

            System.out.println();
        });
    }

    void printView() { printView(null); }

    void printView(Integer nodeIdOpt) {
        Empty empty = Empty.getDefaultInstance();
        if (nodeIdOpt != null && nodeIdOpt > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getStubFromNodeId(nodeIdOpt);
            int nodeId = nodeIdOpt;
            if (stub == null) return;
            try {
                ViewDump vd = ClientApp.timeout(stub).printView(empty);
                System.out.printf("View node %d:%n", nodeId);
                if (vd.getViewsCount() == 0) {
                    System.out.println("  <empty>");
                } else {
                    PaxosProto.NewViewMsg last = vd.getViews(vd.getViewsCount() - 1);
                    PaxosProto.Ballot lb = last.getBallot();
                    System.out.printf("  leader=%d checkpointSeq=%d digest=%s%n",
                            lb.getNodeId(), last.getCheckpointSeq(), last.getCheckpointDigest());
                    int idx = 1;
                    for (NewViewMsg view: vd.getViewsList()) {
                        var b = view.getBallot();
                        System.out.printf("  View %d ballot=(%d,%d) accepts=%d cp=%d digest=%s%n",
                                idx++, b.getRound(), b.getNodeId(), view.getAcceptsCount(),
                                view.getCheckpointSeq(), view.getCheckpointDigest());
                        for (AcceptMsg acc : view.getAcceptsList()) {
                            ClientRequest req = acc.getReq();
                            boolean isNoop = req.getClientId().isEmpty() && req.getTimestamp() == 0 && req.getAmount() == 0;
                            String payload = isNoop
                                    ? "noop"
                                    : String.format("clientId=%s ts=%d %s->%s:%d",
                                    req.getClientId(), req.getTimestamp(),
                                    req.getSender(), req.getReceiver(), req.getAmount());
                            System.out.printf("    seq=%d ballot=(%d,%d) %s%n",
                                    acc.getSeq(), acc.getBallot().getRound(), acc.getBallot().getNodeId(), payload);
                        }
                    }
                }
            }
            catch (Exception e) {
            }
            return;
        }
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = getAllNodesPresent();
        java.util.List<java.util.Map.Entry<NodeServiceGrpc.NodeServiceBlockingStub, Integer>> ordered = new java.util.ArrayList<>(nodes.entrySet());
        ordered.sort(java.util.Comparator.comparingInt(e -> e.getValue() == null ? 0 : e.getValue()));
        for (var entry : ordered) {
            int nodeId = entry.getValue();
            try {
                ViewDump vd = ClientApp.timeout(entry.getKey()).printView(empty);
                System.out.printf("View node %d:%n", nodeId);
                if (vd.getViewsCount() == 0) {
                    System.out.println("  <empty>");
                } else {
                    PaxosProto.NewViewMsg last = vd.getViews(vd.getViewsCount() - 1);
                    PaxosProto.Ballot lb = last.getBallot();
                    System.out.printf("  leader=%d checkpointSeq=%d digest=%s%n",
                            lb.getNodeId(), last.getCheckpointSeq(), last.getCheckpointDigest());
                    int idx = 1;
                    for (NewViewMsg view: vd.getViewsList()) {
                        var b = view.getBallot();
                        System.out.printf("  View %d ballot=(%d,%d) accepts=%d cp=%d digest=%s%n",
                                idx++, b.getRound(), b.getNodeId(), view.getAcceptsCount(),
                                view.getCheckpointSeq(), view.getCheckpointDigest());
                        for (AcceptMsg acc : view.getAcceptsList()) {
                            ClientRequest req = acc.getReq();
                            boolean isNoop = req.getClientId().isEmpty() && req.getTimestamp() == 0 && req.getAmount() == 0;
                            String payload = isNoop
                                    ? "noop"
                                    : String.format("clientId=%s ts=%d %s->%s:%d",
                                    req.getClientId(), req.getTimestamp(),
                                    req.getSender(), req.getReceiver(), req.getAmount());
                            System.out.printf("    seq=%d ballot=(%d,%d) %s%n",
                                    acc.getSeq(), acc.getBallot().getRound(), acc.getBallot().getNodeId(), payload);
                        }
                    }
                }
            } catch (Exception e) {
            }
        }
    }

    LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> allNodesWithIds() {
        return getAllNodesPresent();
    }

    private LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> getAllNodesPresent() {
        LinkedHashMap<NodeServiceGrpc.NodeServiceBlockingStub, Integer> nodes = new LinkedHashMap<>();
        allStubs.forEach(stub -> {
            Integer id = stubNodeIds.get(stub);
            if (id == null || id == 0) {
                int resolved = resolveNodeId(stub);
                if (resolved != 0) id = resolved;
            }
            nodes.put(stub, id == null ? 0 : id);
        });
        return nodes;
    }

    private void syncLivePeers() {
        LivePeers.Builder b = LivePeers.newBuilder();
        for (int nodeId : liveNodeIds) b.addNodeIds(nodeId);
        LivePeers payload = b.build();

        for (var stub : allStubs) {
            Integer id = stubNodeIds.get(stub);
            if (id == null || id == 0) {
                int resolved = resolveNodeId(stub);
                if (resolved != 0) stubNodeIds.putIfAbsent(stub, resolved);
                id = resolved;
            }
            if (id == 0) continue;
            try { ClientApp.timeout(stub).setLivePeers(payload); } catch (Exception ignored) {}
        }
    }


    private boolean isStubAllowed(NodeServiceGrpc.NodeServiceBlockingStub stub) {
        if (liveNodeIds.isEmpty()) return true;
        Integer nodeId = stubNodeIds.get(stub);
        return nodeId != null && nodeId != 0 && liveNodeIds.contains(nodeId);
    }

    private boolean nodeAllowed(int nodeId) {
        return liveNodeIds.isEmpty() || (nodeId != 0 && liveNodeIds.contains(nodeId));
    }

    private NodeServiceGrpc.NodeServiceBlockingStub getStubFromNodeId(int nodeId) {
        if (nodeId <= 0) return null;
        for (var entry : stubNodeIds.entrySet()) {
            if (Objects.equals(entry.getValue(), nodeId)) {
                return entry.getKey();
            }
        }
        for (var entry : stubNodeIds.entrySet()) {
            if (entry.getValue() == null || entry.getValue() == 0) {
                int resolved = resolveNodeId(entry.getKey());
                if (resolved == nodeId) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }


    private int resolveNodeId(NodeServiceGrpc.NodeServiceBlockingStub stub) {
        Integer cached = stubNodeIds.get(stub);
        if (cached != null && cached != 0) return cached;
        try {
            StatusReply info = ClientApp.timeout(stub).getStatus(StatusQuery.newBuilder().setSeq(0).build());
            stubNodeIds.put(stub, info.getNodeId());
            return info.getNodeId();
        } catch (Exception e) {
            stubNodeIds.putIfAbsent(stub, 0);
            return 0;
        }
    }

    private static int parseNodeIdString(String s) {
        if (s == null || s.isBlank()) return 0;
        String digits = s.replaceAll("[^0-9]", "");
        return digits.isEmpty() ? 0 : Integer.parseInt(digits);
    }

    private Map<String, Integer> collectConfiguredNodeIds() {
        Map<String, Integer> map = new LinkedHashMap<>();
        System.getProperties().forEach((k, v) -> {
            String key = String.valueOf(k);
            if (key.startsWith("peer.")) {
                int id = parseNodeIdString(key.substring("peer.".length()));
                if (id > 0) {
                    map.put(String.valueOf(v), id);
                }
            }
        });
        String bootstrapTarget = System.getProperty("bootstrapTarget");
        if (bootstrapTarget != null) {
            map.putIfAbsent(bootstrapTarget, 0);
        }
        String leaderTarget = System.getProperty("leaderTarget");
        if (leaderTarget != null) {
            map.putIfAbsent(leaderTarget, 0);
        }
        return map;
    }

    List<NodeServiceGrpc.NodeServiceBlockingStub> allStubs() {
        return allStubs;
    }

    public NodeServiceGrpc.NodeServiceBlockingStub currentLeaderOrBootstrap() {

        int leaderId = learntLeaderId.get();
        if (leaderId > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub leaderStub = getStubFromNodeId(leaderId);
            if (leaderStub != null && isStubAllowed(leaderStub)) return leaderStub;
        }

        Integer bootId = stubNodeIds.get(bootstrap);
        if (bootId == null || bootId == 0) {
            int resolved = resolveNodeId(bootstrap);
            if (resolved != 0) stubNodeIds.putIfAbsent(bootstrap, resolved);
            bootId = resolved;
        }
        if (nodeAllowed(bootId)) return bootstrap;

        for (NodeServiceGrpc.NodeServiceBlockingStub stub : allStubs) {
            Integer id = stubNodeIds.get(stub);
            if (id == null || id == 0) {
                int resolved = resolveNodeId(stub);
                if (resolved != 0) stubNodeIds.putIfAbsent(stub, resolved);
                id = resolved;
            }
            if (nodeAllowed(id)) return stub;
        }
        return null;
    }



    private int clusterForNodeId(int nodeId) {
        if (nodeId >= 1 && nodeId <= 3) return 1;
        if (nodeId >= 4 && nodeId <= 6) return 2;
        if (nodeId >= 7 && nodeId <= 9) return 3;
        return 0;
    }

    int clusterIdForNodeId(int nodeId) {
        return clusterForNodeId(nodeId);
    }

    private int clusterForAccountId(String accountId) {
        return shardStruct.clusterForAccount(accountId);
    }

    int clusterIdForAccount(String accountId) {
        return clusterForAccountId(accountId);
    }

    NodeServiceGrpc.NodeServiceBlockingStub getPrefStubForAccount(String accountId) {
        int cluster = clusterForAccountId(accountId);
        if (cluster == 0) {
            return currentLeaderOrBootstrap();
        }

        AtomicInteger slot = learntLeaderIdsByCluster[cluster - 1];
        int leaderId = slot.get();
        if (leaderId > 0) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = getStubFromNodeId(leaderId);
            if (stub != null && isStubAllowed(stub)) return stub;
        }

        NodeServiceGrpc.NodeServiceBlockingStub best = null;
        int bestId = 0;
        for (var entry : stubNodeIds.entrySet()) {
            Integer id = entry.getValue();
            if (id == null || id == 0) continue;
            if (clusterForNodeId(id) != cluster) continue;
            if (!nodeAllowed(id)) continue;
            if (best == null || id < bestId) {
                best = entry.getKey();
                bestId = id;
            }
        }
        if (best != null) return best;

        return currentLeaderOrBootstrap();
    }

    List<NodeServiceGrpc.NodeServiceBlockingStub> constructNodeOrderForAccount(String accountId) {
        int cluster = clusterForAccountId(accountId);
        if (cluster == 0) {
            return constructNodeOrder();
        }

        LinkedHashSet<NodeServiceGrpc.NodeServiceBlockingStub> order = new LinkedHashSet<>();

        NodeServiceGrpc.NodeServiceBlockingStub leader = getPrefStubForAccount(accountId);
        if (leader != null && isStubAllowed(leader)) {
            order.add(leader);
        }

        for (NodeServiceGrpc.NodeServiceBlockingStub stub : allStubs) {
            Integer id = stubNodeIds.get(stub);
            if (id == null || id == 0) continue;
            if (clusterForNodeId(id) != cluster) continue;
            if (!isStubAllowed(stub)) continue;
            order.add(stub);
        }

        return new ArrayList<>(order);
    }

    private void log(String fmt, Object... args) {
        PrintHelper.debugf("[Cluster Coordinator] " + fmt, args);
    }


}

