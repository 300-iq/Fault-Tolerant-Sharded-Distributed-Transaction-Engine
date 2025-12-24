# Distributed Banking System – Paxos + 2PC

## How to run


### 1. Start nodes (9-node cluster: 3 clusters × 3 nodes)

Use **nine terminals** (one per node).

In each terminal:

```bash
cd Paxos

# Common peer map for all nodes
COMMON_PEERS="-Dpeer.1=localhost:50051 -Dpeer.2=localhost:50052 -Dpeer.3=localhost:50053 \
              -Dpeer.4=localhost:50054 -Dpeer.5=localhost:50055 -Dpeer.6=localhost:50056 \
              -Dpeer.7=localhost:50057 -Dpeer.8=localhost:50058 -Dpeer.9=localhost:50059 \
              -DdataDir=../data"

# Cluster 1 (C1: nodes 1–3, ports 50051–50053)
./gradlew runNode -DnodeId=1 -Dport=50051 $COMMON_PEERS
./gradlew runNode -DnodeId=2 -Dport=50052 $COMMON_PEERS
./gradlew runNode -DnodeId=3 -Dport=50053 $COMMON_PEERS

# Cluster 2 (C2: nodes 4–6, ports 50054–50056)
./gradlew runNode -DnodeId=4 -Dport=50054 $COMMON_PEERS
./gradlew runNode -DnodeId=5 -Dport=50055 $COMMON_PEERS
./gradlew runNode -DnodeId=6 -Dport=50056 $COMMON_PEERS

# Cluster 3 (C3: nodes 7–9, ports 50057–50059)
./gradlew runNode -DnodeId=7 -Dport=50057 $COMMON_PEERS
./gradlew runNode -DnodeId=8 -Dport=50058 $COMMON_PEERS
./gradlew runNode -DnodeId=9 -Dport=50059 $COMMON_PEERS
```


### 2. Run the client with the provided CSV tests


```bash
cd Paxos
./gradlew runClient \
  --args "src/test/csv tests/CSE535-F25-Project-3-Testcases.csv" \
  -DleaderTarget=localhost:50051 \
  -Dpeer.1=localhost:50051 \
  -Dpeer.2=localhost:50052 \
  -Dpeer.3=localhost:50053 \
  -Dpeer.4=localhost:50054 \
  -Dpeer.5=localhost:50055 \
  -Dpeer.6=localhost:50056 \
  -Dpeer.7=localhost:50057 \
  -Dpeer.8=localhost:50058 \
  -Dpeer.9=localhost:50059
```

This will execute all transactions/failures defined in the CSV against the running nodes.
