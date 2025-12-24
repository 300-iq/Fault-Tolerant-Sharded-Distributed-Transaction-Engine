package paxos.node;
import java.util.Map;


public class NodeConfig {
    private final int nodeId;
    private final int port;
    private final Map<Integer, String> peers;
    private final int leaderId;
    private final int f;


    public NodeConfig(int nodeId, int port, Map<Integer, String> peers, int leaderId, int f) {
        this.nodeId = nodeId;
        this.port = port;
        this.peers = Map.copyOf(peers);
        this.leaderId = leaderId;
        this.f = f;
    }

    public int getNodeId() {
        return nodeId;
    }
    public int getPort() {
        return port;
    }
    public Map<Integer, String> getPeers() {
        return peers;
    }
    public int getLeaderId() {
        return leaderId;
    }
    public int getF() {
        return f;
    }

}