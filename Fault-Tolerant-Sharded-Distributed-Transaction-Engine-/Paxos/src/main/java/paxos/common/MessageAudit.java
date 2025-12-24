package paxos.common;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import paxos.proto.PaxosProto.*;

// This is class if for auditing every request that a node has ever proceesed.
// I took some help of chatGPT to generate the scaffolding.

public final class MessageAudit {

    public enum Dir { SENT, RECV }

    public enum Kind { PREPARE, PROMISE, NEW_VIEW, ACCEPT, ACCEPTED, COMMIT, HEARTBEAT, EXECUTE, LEADER, CHECKPOINT }

    public static final class Entry {
        public final long ts;
        public final int  selfNodeId;
        public final Dir  dir;
        public final Kind kind;

        public final int  peerNodeId;
        public final int  ballotRound;
        public final int  ballotNode;
        public final int  seq;


        public final String clientId;
        public final long   reqTs;
        public final String sender;
        public final String receiver;
        public final long   amount;

        public final String note;

        public Entry(
                long ts, int selfNodeId, Dir dir, Kind kind,
                int peerNodeId, int ballotRound, int ballotNode, int seq,
                String clientId, long reqTs, String sender, String receiver, long amount,
                String note) {
            this.ts = ts;
            this.selfNodeId  = selfNodeId;
            this.dir         = Objects.requireNonNull(dir);
            this.kind        = Objects.requireNonNull(kind);
            this.peerNodeId  = peerNodeId;
            this.ballotRound = ballotRound;
            this.ballotNode  = ballotNode;
            this.seq         = seq;
            this.clientId    = clientId;
            this.reqTs       = reqTs;
            this.sender      = sender;
            this.receiver    = receiver;
            this.amount      = amount;
            this.note        = note;
        }
    }

    private static final int DEFAULT_MAX_ENTRIES = 50_000;

    private final int selfNodeId;
    private final int maxEntries;
    private final ConcurrentLinkedQueue<Entry> q = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger(0);

    public MessageAudit(int selfNodeId) {
        this(selfNodeId, DEFAULT_MAX_ENTRIES);
    }

    public MessageAudit(int selfNodeId, int maxEntries) {
        this.selfNodeId = selfNodeId;
        this.maxEntries = maxEntries;
    }

    public void logIt(Dir dir, Kind kind,
                      Integer peerNodeId,
                      Ballot ballot,
                      Integer seq,
                      ClientRequest req,
                      String note) {
        if (maxEntries <= 0) {
            return;
        }
        final long now = Instant.now().toEpochMilli();
        final int  peer   = (peerNodeId == null ? -1 : peerNodeId);
        final int  bRound = (ballot == null ? -1 : ballot.getRound());
        final int  bNode  = (ballot == null ? -1 : ballot.getNodeId());
        final int  s      = (seq == null ? -1 : seq);

        String clientId = null, sender = null, receiver = null;
        long reqTs = 0L, amount = 0L;
        if (req != null) {
            clientId = emptyToNull(req.getClientId());
            reqTs    = req.getTimestamp();
            sender   = emptyToNull(req.getSender());
            receiver = emptyToNull(req.getReceiver());
            amount   = req.getAmount();
        }

        Entry e = new Entry(
                now, selfNodeId, dir, kind,
                peer, bRound, bNode, s,
                clientId, reqTs, sender, receiver, amount,
                note
        );
        q.add(e);

        int cur = size.incrementAndGet();
        if (maxEntries > 0 && cur > maxEntries) {
            while (size.get() > maxEntries) {
                Entry dropped = q.poll();
                if (dropped == null) break;
                size.decrementAndGet();
            }
        }
    }

    public List<Entry> snapshot() {
        return new ArrayList<>(q);
    }

    public static boolean isNoop(String clientId, long ts, long amount) {
        return (clientId == null || clientId.isEmpty()) && ts == 0L && amount == 0L;
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    public static void log(
            paxos.node.NodeState state,
            MessageAudit.Dir dir,
            MessageAudit.Kind kind,
            Integer peerNodeId,
            Ballot ballot,
            Integer seq,
            ClientRequest req,
            String note
    ) {
        state.getAudit().logIt(dir, kind, peerNodeId, ballot, seq, req, note);
    }
}
