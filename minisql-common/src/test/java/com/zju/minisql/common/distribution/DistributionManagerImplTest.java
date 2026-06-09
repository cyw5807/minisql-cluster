package com.zju.minisql.common.distribution;

import com.zju.minisql.common.cluster.ClusterEvent;
import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DistributionManagerImplTest {

    /**
     * 场景 T1：写请求路由 + 节点下线后缓存失效
     * 集群初始有两个节点；路由一次写请求验证能正常返回节点。
     * 随后将第一个节点标记为移除并触发 onClusterChange(NODE_REMOVED)，
     * 验证后续路由仍能返回非空节点（路由缓存已失效，哈希环已移除该节点）。
     */
    @Test
    void shouldRouteAndInvalidateAfterClusterChange() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9012"));
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9013"));

        DistributionManagerImpl manager = new DistributionManagerImpl(metadata, 8, 1000);
        NodeInfo nodeA = manager.routeForWrite("user_1");
        Assertions.assertNotNull(nodeA);

        NodeInfo removed = metadata.alive.get(0);
        metadata.alive.remove(removed);
        manager.onClusterChange(ClusterEvent.removed(removed));

        NodeInfo nodeB = manager.routeForWrite("user_1");
        Assertions.assertNotNull(nodeB);
    }

    /**
     * 场景 T2：路由缓存 TTL 内命中
     * 元数据中配置好 Primary 节点，TTL 设为 30s。
     * 对相同 key 连续路由两次，验证第二次命中本地缓存，
     * ZooKeeper（InMemoryMetadata）的 getPrimaryNode 只被调用一次，
     * 说明路由缓存（RoutingTable）生效，减少了元数据查询开销。
     */
    @Test
    void shouldHitRoutingCacheWithinTtl() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        NodeInfo primary = NodeInfo.fromAddress("127.0.0.1:9012");
        metadata.primary = primary;
        metadata.alive.add(primary);
        DistributionManager manager = new DistributionManagerImpl(metadata, 8, 30_000);

        NodeInfo first = manager.routeForRead("cache-key");
        NodeInfo second = manager.routeForRead("cache-key");
        Assertions.assertEquals(primary, first);
        Assertions.assertEquals(primary, second);
        // 路由缓存命中，ZK 查询仅触发一次
        Assertions.assertEquals(1, metadata.primaryQueryCount);
    }

    /**
     * 场景 T3：getPartitionNodes 返回 Primary + Replica 完整列表
     * 元数据中为同一分区配置了一个 Primary 和一个 Replica。
     * 验证 getPartitionNodes 返回的列表长度为 2，
     * 且顺序为 Primary 在前、Replica 在后（写操作首选 Primary 保证一致性）。
     */
    @Test
    void shouldReturnPrimaryAndReplicasForPartitionNodes() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        NodeInfo primary = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo replica = NodeInfo.fromAddress("127.0.0.1:9013");
        metadata.primary = primary;
        metadata.replicas = List.of(replica);
        DistributionManager manager = new DistributionManagerImpl(metadata, 8, 30_000);

        List<NodeInfo> nodes = manager.getPartitionNodes(1);
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertEquals(primary, nodes.get(0));
        Assertions.assertEquals(replica, nodes.get(1));
    }

    /**
     * 场景 T4：Primary 缺失时回退到一致性哈希环
     * 元数据中不配置 Primary（getPrimaryNode 返回 null），
     * 集群有两个存活节点。
     * 验证路由仍能通过一致性哈希环返回非空节点，
     * 且返回的节点一定是存活节点之一（保证路由不会把请求发到未知节点）。
     */
    @Test
    void shouldFallbackToHashRingWhenPrimaryIsAbsent() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9012"));
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9013"));
        DistributionManager manager = new DistributionManagerImpl(metadata, 32, 30_000);

        NodeInfo routed = manager.routeForWrite("fallback-key");
        Assertions.assertNotNull(routed);
        Assertions.assertTrue(metadata.alive.contains(routed));
    }

    private static class InMemoryMetadata implements MetadataService {
        private final List<NodeInfo> alive = new ArrayList<>();
        private NodeInfo primary;
        private List<NodeInfo> replicas = List.of();
        private int primaryQueryCount;

        @Override
        public NodeInfo getPrimaryNode(int partitionId) {
            primaryQueryCount++;
            return primary;
        }

        @Override
        public List<NodeInfo> getAllReplicas(int partitionId) {
            return replicas;
        }

        @Override
        public List<NodeInfo> getAllAliveNodes() {
            return new ArrayList<>(alive);
        }

        @Override
        public void updatePartitionOwner(int partitionId, NodeInfo newPrimary) {
            // no-op
        }
    }
}
