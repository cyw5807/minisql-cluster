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

    /**
     * 场景 T1：正常写入 + 读取路由 + Primary 故障切换
     * 集群：Primary=9012，Replica=9013/9014。
     * 1. write 验证写入成功（Primary 写 + 两副本 ack 满足 quorum）
     * 2. read 验证返回的路由节点包含集群 IP（说明成功路由到某个副本）
     * 3. 通知 9012 下线后，验证新 Primary 不再是 9012（FailoverHandler 已选出继任者）
     */
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

    /**
     * 场景 T2：Primary 写失败时自动切换并重试
     * MockTransport 配置 9012 写入总是返回 fail。
     * 第一次写入 Primary(9012) 失败 → ReplicaManagerImpl 触发 onNodeFailure(9012)
     * → FailoverHandler 从存活副本中选出新 Primary(9013/9014)
     * → 自动重试写入到新 Primary，最终写入成功。
     * 验证故障切换路径下整体写入的可用性。
     */
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
        // 切换后 Primary 已不再是 9012
        Assertions.assertNotEquals("127.0.0.1:9012", metadata.getPrimaryNode(partitionId).getNodeId());
    }

    /**
     * 场景 T3：Quorum 写入（一个副本写失败仍成功）
     * MockTransport 配置 9014 写入总是返回 fail。
     * 集群 3 节点，quorum = max(1, floor(2/2)) + 1 = 2。
     * Primary(9012) 成功 + Replica(9013) 成功 = ackCount=2 ≥ quorum=2，
     * 即使 9014 失败，整体写入仍返回 success。
     * 验证 quorum 机制的容错能力。
     */
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

    /**
     * 场景 T4：选举优先级 —— 同步进度最大的副本晋升为 Primary
     * 预先调用 FailoverHandler.recordSync 记录：9013 同步进度=100，9014 同步进度=200。
     * Primary(9012) 下线后，FailoverHandler 应选择 9014（进度更大，数据更完整）作为新 Primary。
     * 验证副本选主策略遵循"最新数据优先"原则，减少数据丢失风险。
     */
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
        // 9014 同步进度更大，应被选为新 Primary
        Assertions.assertEquals(r2, metadata.getPrimaryNode(partitionId));
    }

    /**
     * 场景 T5：被动 Gap 检测 + syncWithRecovery 补偿
     * 模拟副本网络抖动导致 logIndex=1 的条目未到达副本（只写到 Primary）。
     * 下一条 logIndex=2 到来时，GapAwareTransport 返回 ReplicaSyncAck.gap(expectedLogIndex=1)。
     * PrimaryHandler.syncWithRecovery 捕获 gap 后：
     *   1. 调用 MissingLogProvider 加载 [1..2] 的缺失日志
     *   2. 调用 recover 把缺失条目推送给副本
     *   3. 重试 appendEntry(logIndex=2)
     * 验证两个副本最终 appliedIndex 均为 2，即缺口已自动补齐。
     */
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

        // 第一条写入时模拟两个副本均未收到（仅 Primary 写成功），制造缺口
        transport.setDropReplicasOnce(true);
        Assertions.assertTrue(replicaManager.write(partitionId, "student", Row.of("id", 5001, "name", "first")).isSuccess());

        // 第二条写入时副本返回 gap，触发 syncWithRecovery 补齐缺失条目后重试
        transport.setDropReplicasOnce(false);
        Assertions.assertTrue(replicaManager.write(partitionId, "student", Row.of("id", 5002, "name", "second")).isSuccess());

        // 两个副本的 appliedIndex 均应为 2（缺口已补齐）
        Assertions.assertEquals(2L, transport.getAppliedIndex(r1.getNodeId()));
        Assertions.assertEquals(2L, transport.getAppliedIndex(r2.getNodeId()));
    }

    /**
     * 场景 T6：节点重连后全量日志追平（onNodeRecovery 主动推送）
     * 9012 宕机期间，Primary(9013) 写入 logIndex=1 和 logIndex=2 共2条记录，
     * 9012 的 appliedIndex 始终为 0。
     * 9012 重新上线后调用 onNodeRecovery，
     * ReplicaManagerImpl 遍历 partitionLogs，发现 9012 缺失 [1,2]，
     * 通过 primaryHandler.recoverReplica 将两条日志全量推送给 9012。
     * 验证 9012 的 appliedIndex 最终追平至 2。
     */
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

        // 9012 宕机期间写入两条记录
        transport.setUnavailable(recovering.getNodeId(), true);
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 6001, "course", "A")).isSuccess());
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 6002, "course", "B")).isSuccess());
        Assertions.assertEquals(0L, transport.getAppliedIndex(recovering.getNodeId()));

        // 9012 重新上线，触发主动全量追平
        transport.setUnavailable(recovering.getNodeId(), false);
        replicaManager.onNodeRecovery(recovering.getNodeId());
        // 追平后 appliedIndex 应为 2
        Assertions.assertEquals(2L, transport.getAppliedIndex(recovering.getNodeId()));
    }

    /**
     * 场景 T7：恢复重试机制 —— 首次 recover 调用失败后重试成功
     * FlakyRecoveryTransport 模拟节点重新上线时服务尚未就绪：
     * 对指定节点的第一次 recover 调用返回 fail，第二次及以后正常。
     * onNodeRecovery 中的 recoverWithRetry（最多5次，间隔200ms）应在第2次重试时成功。
     * 验证恢复重试机制能应对 Worker 服务启动延迟，保证数据最终一致。
     */
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

        // 9012 宕机期间写入1条记录
        transport.setUnavailable(recovering.getNodeId(), true);
        Assertions.assertTrue(replicaManager.write(partitionId, "score", Row.of("id", 7001, "course", "X")).isSuccess());
        transport.setUnavailable(recovering.getNodeId(), false);

        // 首次 recover 失败，第2次重试成功
        replicaManager.onNodeRecovery(recovering.getNodeId());
        Assertions.assertEquals(1L, transport.getAppliedIndex(recovering.getNodeId()));
    }

    /**
     * 场景 T8：节点不在 ZK 副本元数据中时仍能正常恢复（复现线上 id=1001 缺失场景）
     * 背景：9012 原本是 Primary，故障切换后 ZK 将 9013 设为新 Primary，
     * 但元数据中 replicas 列表只有 9014，没有 9012（旧 Primary 未被回写为 replica）。
     * 若 onNodeRecovery 只按元数据过滤分区，则 partition=8 的恢复会被跳过。
     * 修复后：遍历 partitionLogs 中所有分区（不依赖元数据），9012 仍可追平 logIndex=1/2。
     * 验证修复后 appliedIndex 最终为 2，即不再出现数据缺失。
     */
    @Test
    void shouldRecoverNodeEvenWhenNotInReplicaMetadata() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        int partitionId = 8;
        NodeInfo primary = NodeInfo.fromAddress("127.0.0.1:9013");
        NodeInfo recovering = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo replica = NodeInfo.fromAddress("127.0.0.1:9014");
        metadata.owner.put(partitionId, primary);
        // 故障切换后元数据未把旧 Primary(9012) 放回 replicas，复现线上缺数据的场景
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

        // 即使 9012 不在副本元数据中，onNodeRecovery 也应遍历 partitionLogs 完成恢复
        replicaManager.onNodeRecovery(recovering.getNodeId());
        Assertions.assertEquals(2L, transport.getAppliedIndex(recovering.getNodeId()));
    }

    // -------------------- Helper stubs --------------------

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

    /** 全部节点均成功或按指定节点集合失败的简单 transport stub */
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

    /**
     * 能模拟副本漏收条目（产生 gap）并在 recover 时补齐的 transport stub。
     * dropReplicasOnce=true 时副本 syncWrite 不实际推进 appliedIndex，制造缺口。
     */
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
                // 模拟副本未收到该条目：返回 ok 但不推进 appliedIndex
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

    /**
     * 支持节点临时不可用（模拟宕机）的 transport stub。
     * unavailable 期间 syncWrite 和 recover 均返回 fail；
     * 恢复后按正常 logIndex 连续性写入。
     */
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
                    continue; // 幂等：已应用的条目跳过
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

    /**
     * 在 RecoveryAwareTransport 基础上，对指定节点的首次 recover 调用返回 fail，
     * 模拟节点刚启动时 RPC 服务尚未就绪的场景。
     */
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
