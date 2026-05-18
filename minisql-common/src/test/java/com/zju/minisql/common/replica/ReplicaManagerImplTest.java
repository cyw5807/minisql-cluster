package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.loadbalance.LoadBalancer;
import com.zju.minisql.common.query.model.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                new PrimaryHandler(new MockTransport()),
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
                new PrimaryHandler(new MockTransport(Set.of("127.0.0.1:9012"))),
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
                new PrimaryHandler(new MockTransport(Set.of("127.0.0.1:9014"))),
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
                new PrimaryHandler(new MockTransport()),
                new ReplicaHandler(new SimpleLoadBalancer()),
                failoverHandler
        );

        replicaManager.read(partitionId, "warmup-key");
        replicaManager.onNodeFailure(p.getNodeId());
        Assertions.assertEquals(r2, metadata.getPrimaryNode(partitionId));
    }

    @Test
    void shouldRecoverMissingEntriesWhenGapDetected() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 5;
        NodeInfo p = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo r1 = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo r2 = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, p);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(r1, r2)));

        GapAwareTransport transport = new GapAwareTransport();
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler(transport),
                new ReplicaHandler(new SimpleLoadBalancer()),
                new FailoverHandler(metadata)
        );

        // 第一条仅写到 primary，模拟网络抖动导致两个副本都漏掉 index=1
        transport.setDropReplicasOnce(true);
        Assertions.assertTrue(replicaManager.write(partitionId, "student", Row.of("id", 5001, "name", "first")).isSuccess());
        // 第二条到来时副本应返回 gap，触发 recover 后再成功写 index=2
        transport.setDropReplicasOnce(false);
        Assertions.assertTrue(replicaManager.write(partitionId, "student", Row.of("id", 5002, "name", "second")).isSuccess());
        Assertions.assertEquals(2L, transport.getAppliedIndex(r1.getNodeId()));
        Assertions.assertEquals(2L, transport.getAppliedIndex(r2.getNodeId()));
    }

    @Test
    void shouldCatchUpRecoveredNodeWithFullReplay() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 6;
        NodeInfo primary = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo recovering = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo replica = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, primary);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(recovering, replica)));

        RecoveryAwareTransport transport = new RecoveryAwareTransport();
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler(transport),
                new ReplicaHandler(new SimpleLoadBalancer()),
                new FailoverHandler(metadata)
        );

        transport.setUnavailable(recovering.getNodeId(), true);
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 6001, "course", "A")).isSuccess());
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 6002, "course", "B")).isSuccess());
        Assertions.assertEquals(0L, transport.getAppliedIndex(recovering.getNodeId()));

        transport.setUnavailable(recovering.getNodeId(), false);
        replicaManager.onNodeRecovery(recovering.getNodeId());
        Assertions.assertEquals(2L, transport.getAppliedIndex(recovering.getNodeId()));
    }

    @Test
    void shouldRetryRecoveryWhenFirstRecoverAttemptFails() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 7;
        NodeInfo primary = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo recovering = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo replica = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, primary);
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(recovering, replica)));

        FlakyRecoveryTransport transport = new FlakyRecoveryTransport(recovering.getNodeId());
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler(transport),
                new ReplicaHandler(new SimpleLoadBalancer()),
                new FailoverHandler(metadata)
        );

        transport.setUnavailable(recovering.getNodeId(), true);
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 7001, "course", "X")).isSuccess());
        transport.setUnavailable(recovering.getNodeId(), false);

        replicaManager.onNodeRecovery(recovering.getNodeId());
        Assertions.assertEquals(1L, transport.getAppliedIndex(recovering.getNodeId()));
    }

    @Test
    void shouldRecoverNodeEvenWhenNotInReplicaMetadata() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 8;
        NodeInfo primary = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo recovering = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo replica = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, primary);
        // 模拟 failover 后元数据未把旧主(9012)放回 replicas，复现线上缺 1001 的场景
        metadata.replicas.put(partitionId, new ArrayList<>(List.of(replica)));

        RecoveryAwareTransport transport = new RecoveryAwareTransport();
        ReplicaManager replicaManager = new ReplicaManagerImpl(
                metadata,
                new PrimaryHandler(transport),
                new ReplicaHandler(new SimpleLoadBalancer()),
                new FailoverHandler(metadata)
        );

        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 8001, "course", "A")).isSuccess());
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 8002, "course", "B")).isSuccess());
        Assertions.assertEquals(0L, transport.getAppliedIndex(recovering.getNodeId()));

        replicaManager.onNodeRecovery(recovering.getNodeId());
        Assertions.assertEquals(2L, transport.getAppliedIndex(recovering.getNodeId()));
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

    private static class MockTransport implements ReplicaSyncTransport {
        private final Set<String> failNodes;

        private MockTransport() {
            this(Set.of());
        }

        private MockTransport(Set<String> failNodes) {
            this.failNodes = new HashSet<>(failNodes);
        }

        @Override
        public ReplicaSyncAck syncWrite(NodeInfo nodeInfo, ReplicationLogEntry entry) {
            if (failNodes.contains(nodeInfo.getNodeId())) {
                return ReplicaSyncAck.fail("mock fail");
            }
            return ReplicaSyncAck.ok(entry.getLogIndex() + 1, "ok");
        }

        @Override
        public ReplicaSyncAck recover(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
            if (failNodes.contains(nodeInfo.getNodeId())) {
                return ReplicaSyncAck.fail("mock fail");
            }
            long next = entries.isEmpty() ? 1L : entries.get(entries.size() - 1).getLogIndex() + 1;
            return ReplicaSyncAck.ok(next, "recover ok");
        }

        @Override
        public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
            return 0L;
        }
    }

    private static class GapAwareTransport implements ReplicaSyncTransport {
        private final Map<String, Long> applied = new HashMap<>();
        private boolean dropReplicasOnce;

        public void setDropReplicasOnce(boolean dropReplicasOnce) {
            this.dropReplicasOnce = dropReplicasOnce;
        }

        public long getAppliedIndex(String nodeId) {
            return applied.getOrDefault(nodeId, 0L);
        }

        @Override
        public ReplicaSyncAck syncWrite(NodeInfo nodeInfo, ReplicationLogEntry entry) {
            long current = applied.getOrDefault(nodeInfo.getNodeId(), 0L);
            long expected = current + 1;
            if (dropReplicasOnce && !"127.0.0.1:9012".equals(nodeInfo.getNodeId())) {
                return ReplicaSyncAck.ok(expected, "simulate drop");
            }
            if (entry.getLogIndex() != expected) {
                return ReplicaSyncAck.gap(expected, "gap");
            }
            applied.put(nodeInfo.getNodeId(), entry.getLogIndex());
            return ReplicaSyncAck.ok(entry.getLogIndex() + 1, "ok");
        }

        @Override
        public ReplicaSyncAck recover(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
            long current = applied.getOrDefault(nodeInfo.getNodeId(), 0L);
            long expected = current + 1;
            for (ReplicationLogEntry entry : entries) {
                if (entry.getLogIndex() != expected) {
                    return ReplicaSyncAck.gap(expected, "recover gap");
                }
                applied.put(nodeInfo.getNodeId(), entry.getLogIndex());
                expected++;
            }
            return ReplicaSyncAck.ok(expected, "recover ok");
        }

        @Override
        public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
            return applied.getOrDefault(nodeInfo.getNodeId(), 0L);
        }
    }

    private static class RecoveryAwareTransport implements ReplicaSyncTransport {
        private final Map<String, Long> applied = new HashMap<>();
        private final Set<String> unavailable = new HashSet<>();

        public void setUnavailable(String nodeId, boolean down) {
            if (down) {
                unavailable.add(nodeId);
            } else {
                unavailable.remove(nodeId);
            }
        }

        public long getAppliedIndex(String nodeId) {
            return applied.getOrDefault(nodeId, 0L);
        }

        @Override
        public ReplicaSyncAck syncWrite(NodeInfo nodeInfo, ReplicationLogEntry entry) {
            if (unavailable.contains(nodeInfo.getNodeId())) {
                return ReplicaSyncAck.fail("unavailable");
            }
            long expected = applied.getOrDefault(nodeInfo.getNodeId(), 0L) + 1;
            if (entry.getLogIndex() != expected) {
                return ReplicaSyncAck.gap(expected, "gap");
            }
            applied.put(nodeInfo.getNodeId(), entry.getLogIndex());
            return ReplicaSyncAck.ok(entry.getLogIndex() + 1, "ok");
        }

        @Override
        public ReplicaSyncAck recover(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
            if (unavailable.contains(nodeInfo.getNodeId())) {
                return ReplicaSyncAck.fail("unavailable");
            }
            long expected = applied.getOrDefault(nodeInfo.getNodeId(), 0L) + 1;
            for (ReplicationLogEntry entry : entries) {
                if (entry.getLogIndex() < expected) {
                    continue;
                }
                if (entry.getLogIndex() > expected) {
                    return ReplicaSyncAck.gap(expected, "recover gap");
                }
                applied.put(nodeInfo.getNodeId(), entry.getLogIndex());
                expected++;
            }
            return ReplicaSyncAck.ok(expected, "recover ok");
        }

        @Override
        public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
            return applied.getOrDefault(nodeInfo.getNodeId(), 0L);
        }
    }

    private static class FlakyRecoveryTransport extends RecoveryAwareTransport {
        private final String flakyNodeId;
        private final Set<String> failedOnce = new HashSet<>();

        private FlakyRecoveryTransport(String flakyNodeId) {
            this.flakyNodeId = flakyNodeId;
        }

        @Override
        public ReplicaSyncAck recover(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
            if (nodeInfo.getNodeId().equals(flakyNodeId) && !failedOnce.contains(flakyNodeId)) {
                failedOnce.add(flakyNodeId);
                return ReplicaSyncAck.fail("simulated first recover failure");
            }
            return super.recover(nodeInfo, partitionId, entries);
        }
    }
}
