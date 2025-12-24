package paxos.node;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import paxos.proto.NodeServiceGrpc;
import paxos.rpc.GrpcService;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NodeServer {
    private Server grpcServer;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    private final Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> stubs = new ConcurrentHashMap<>();


    private final Map<Integer, NodeServiceGrpc.NodeServiceBlockingStub> twoPcStubs = new ConcurrentHashMap<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final NodeState state;
    private Thread execThread;

    public NodeServer(NodeConfig cfg, Collection<String> accounts, int initBal) {
        this.state = new NodeState(cfg, accounts, initBal);
    }


    public void start() throws IOException {


        state.getNodeConfig().getPeers().forEach((id, address) -> {
            if (id != state.getNodeConfig().getNodeId()) {
                ManagedChannel ch = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
                channels.add(ch);
                NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(ch);
                stubs.put(id, stub);
            }
        });


        twoPcStubs.putAll(stubs);


        int selfId = state.getNodeConfig().getNodeId();
        java.util.Map<Integer, String> allPeers = new java.util.HashMap<>();
        System.getProperties().forEach((k, v) -> {
            String ks = String.valueOf(k);
            if (ks.startsWith("peer.")) {
                try {
                    int id = Integer.parseInt(ks.substring("peer.".length()));
                    allPeers.put(id, String.valueOf(v));
                } catch (NumberFormatException ignored) { }
            }
        });
        if (!allPeers.containsKey(selfId)) {
            allPeers.put(selfId, "localhost:" + state.getNodeConfig().getPort());
        }

        allPeers.forEach((id, address) -> {
            if (id == selfId) return;
            if (twoPcStubs.containsKey(id)) return;
            ManagedChannel ch = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
            channels.add(ch);
            NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(ch);
            twoPcStubs.put(id, stub);
        });

        int n = state.getNodeConfig().getPeers().size();
        int quorum = (n / 2) + 1;

        Replication replication = new Replication(state, stubs, quorum, threadPool);
        StateMachine stateMachine = new StateMachine(state);

        long heartbeat  = Long.getLong("heartbeat",   200L);
        long minLeaderTimeout = Long.getLong("minLeaderTimeout", 600L);
        long maxLeaderTimeout = Long.getLong("maxLeaderTimeout", 950L);
        long minPrepareBackoff = Long.getLong("minPrepareBackoff", 150L);
        long maxPrepareBackoff = Long.getLong("maxPrepareBackoff", 300L);
        long replicationTimeout = Long.getLong("replicationTimeout", 1200L);
        long tp  = Long.getLong("prepareThrottle",   250L);

        ElectionCoordinator electionCoordinator = new ElectionCoordinator(
                state, replication, stateMachine, stubs, threadPool, quorum,
                heartbeat, minLeaderTimeout, maxLeaderTimeout, minPrepareBackoff, maxPrepareBackoff, replicationTimeout, tp);

        electionCoordinator.start();

        electionCoordinator.probeElectionSoon();


        Thread execThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    stateMachine.executeStateMachine();
                    try {
                        Thread.sleep(1L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (Throwable ignored) {
            }
        }, "state-exec-" + state.getNodeConfig().getNodeId());
        execThread.setDaemon(true);
        execThread.start();
        this.execThread = execThread;

        GrpcService svc = new GrpcService(state, replication, stateMachine, stubs, twoPcStubs, electionCoordinator);


        grpcServer = ServerBuilder.forPort(state.getNodeConfig().getPort())
                .addService(svc)
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (grpcServer != null) grpcServer.shutdown().awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
            channels.forEach(ch -> {
                try { ch.shutdownNow().awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            });
            if (electionCoordinator != null) {
                electionCoordinator.shutdown();
            }
            if (svc != null) {
                try { svc.shutdown(); } catch (Exception ignored) {}
            }
            if (execThread != null) {
                execThread.interrupt();
            }
            threadPool.shutdownNow();
        }, "shutdown-" + state.getNodeConfig().getNodeId()));

        System.out.printf("Node %d up @ %d leader=%s ballot=%s quorum=%d/%d\n",
                state.getNodeConfig().getNodeId(), state.getNodeConfig().getPort(), state.getIsLeader(), state.getCurrentBallot(), quorum, n);
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) grpcServer.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        int nodeId = Integer.parseInt(System.getProperty("nodeId", "1"));
        int port = Integer.parseInt(System.getProperty("port", "50051"));

        Map<Integer, String> allPeers = new HashMap<>();
        System.getProperties().forEach((k, v) -> {
            String ks = String.valueOf(k);
            if (ks.startsWith("peer.")) {
                int id = Integer.parseInt(ks.substring("peer.".length()));
                allPeers.put(id, String.valueOf(v));
            }
        });
        if (!allPeers.containsKey(nodeId)) allPeers.put(nodeId, "localhost:" + port);

        Map<Integer, String> peers = new HashMap<>();
        allPeers.forEach((id, addr) -> {
            if (inSameCluster(nodeId, id)) {
                peers.put(id, addr);
            }
        });

        int leaderId = initialClusterLeader(nodeId);

        NodeConfig cfg = new NodeConfig(nodeId, port, peers, leaderId, 1);
        List<String> accounts = buildShardAccountsForNode(nodeId);
        NodeServer ns = new NodeServer(cfg, accounts, 10);
        ns.start();
        ns.blockUntilShutdown();
    }

    private static boolean inSameCluster(int nodeId, int id) {

        String clusterId = getClusterId(nodeId);
        return clusterId.equals(getClusterId(id));
    }

    private static String getClusterId(int nodeId) {

        if (nodeId >= 1 && nodeId <= 3) return "C1";
        if (nodeId >= 4 && nodeId <= 6) return "C2";
        if (nodeId >= 7 && nodeId <= 9) return "C3";
        return null;
    }

    private static int initialClusterLeader(int nodeId) {

        String clusterId = getClusterId(nodeId);
        if (clusterId.equals("C1")) return 1;
        if (clusterId.equals("C2")) return 4;
        if (clusterId.equals("C3")) return 7;
        return -1;
    }

    private static List<String> buildShardAccountsForNode(int nodeId) {
        int start;
        int end;
        if (nodeId >= 1 && nodeId <= 3) {

            start = 1;
            end = 3000;
        } else if (nodeId >= 4 && nodeId <= 6) {

            start = 3001;
            end = 6000;
        } else if (nodeId >= 7 && nodeId <= 9) {

            start = 6001;
            end = 9000;
        } else {
            throw new IllegalArgumentException("Unsupported nodeId " + nodeId + " for fixed 3x3 cluster layout");
        }

        List<String> accounts = new ArrayList<>(end - start + 1);
        for (int i = start; i <= end; i++) {
            accounts.add(Integer.toString(i));
        }
        return accounts;
    }
}
