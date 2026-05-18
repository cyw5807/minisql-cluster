package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.loadbalance.LoadBalancer;
import com.zju.minisql.common.query.model.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ReplicaManagerImplTest {

    @Test
    void shouldWriteReadAndFailover() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 1;
        NodeInfo p = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo r1 = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo r2 = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, p);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(r1, r2)));

        LoadBalancer lb = new SimpleLoadBalancer();
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler((nodeInfo, pid, tableName, row) -> true),
                new ReplicaHandler(lb),
                new FailoverHandler(metadata)
        );

        ReplicaResult writeResult = replicaManager.write(partitionId, "student", Row.of("id", 1001, "name", "alice"));
        Assertions.assertTrue(writeResult.isSuccess());

        ReplicaResult readResult = replicaManager.read(partitionId, "1001");
        Assertions.assertTrue(readResult.isSuccess());
        Assertions.assertTrue(readResult.getMessage().contains("127.0.0.1"));

        replicaManager.onNodeFailure("127.0.0.1:9012");
        Assertions.assertNotNull(metadata.getPrimaryNode(partitionId));
        Assertions.assertNotEquals("127.0.0.1:9012", metadata.getPrimaryNode(partitionId).getNodeId());
    }

    @Test
    void shouldAutoFailoverAndRetryWhenPrimaryWriteFails() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 2;
        NodeInfo p = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo r1 = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo r2 = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, p);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(r1, r2)));

        LoadBalancer lb = new SimpleLoadBalancer();
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler((nodeInfo, pid, tableName, row) -> !"127.0.0.1:9012".equals(nodeInfo.getNodeId())),
                new ReplicaHandler(lb),
                new FailoverHandler(metadata)
        );

        ReplicaResult writeResult = replicaManager.write(partitionId, "student", Row.of("id", 2001, "name", "bob"));
        Assertions.assertTrue(writeResult.isSuccess());
        Assertions.assertNotEquals("127.0.0.1:9012", metadata.getPrimaryNode(partitionId).getNodeId());
    }

    @Test
    void shouldMeetQuorumWhenOneReplicaWriteFails() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 3;
        NodeInfo p = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo r1 = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo r2 = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, p);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(r1, r2)));

        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler((nodeInfo, pid, tableName, row) -> !"127.0.0.1:9014".equals(nodeInfo.getNodeId())),
                new ReplicaHandler(new SimpleLoadBalancer()),
                new FailoverHandler(metadata)
        );

        ReplicaResult writeResult = replicaManager.write(partitionId, "student", Row.of("id", 3001, "name", "carol"));
        Assertions.assertTrue(writeResult.isSuccess());
    }

    @Test
    void shouldPromoteReplicaWithLargestSyncProgress() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 4;
        NodeInfo p = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo r1 = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo r2 = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, p);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(r1, r2)));

        FailoverHandler failoverHandler = new FailoverHandler(metadata);
        failoverHandler.recordSync(r1.getNodeId(), 100L);
        failoverHandler.recordSync(r2.getNodeId(), 200L);
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler((nodeInfo, pid, tableName, row) -> true),
                new ReplicaHandler(new SimpleLoadBalancer()),
                failoverHandler
        );

        replicaManager.read(partitionId, "warmup-key");
        replicaManager.onNodeFailure(p.getNodeId());
        Assertions.assertEquals(r2, metadata.getPrimaryNode(partitionId));
    }

    private static class SimpleLoadBalancer implements LoadBalancer {
        @Override
        public NodeInfo selectReadNode(int partitionId, List<NodeInfo> nodes) {
            return nodes.get(Math.min(1, nodes.size() - 1));
        }

        @Override
        public void reportNodeLoad(String nodeId, com.zju.minisql.common.cluster.NodeLoad load) {
            // no-op
        }
    }

    private static class InMemoryMetadata implements MetadataService {
        private final Map<Integer, NodeInfo> owner = new HashMap<>();
        private final Map<Integer, List<NodeInfo>> replicas = new HashMap<>();

        @Override
        public NodeInfo getPrimaryNode(int partitionId) {
            return owner.get(partitionId);
        }

        @Override
        public List<NodeInfo> getAllReplicas(int partitionId) {
            return replicas.getOrDefault(partitionId, List.of());
        }

        @Override
        public List<NodeInfo> getAllAliveNodes() {
            List<NodeInfo> nodes = new ArrayList<>(owner.values());
            replicas.values().forEach(nodes::addAll);
            return nodes;
        }

        @Override
        public void updatePartitionOwner(int partitionId, NodeInfo newPrimary) {
            owner.put(partitionId, newPrimary);
        }
    }
}
