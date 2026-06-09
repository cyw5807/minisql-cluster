package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.NodeLoad;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.distribution.DistributionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LoadBalancerImplTest {

    /**
     * 场景 T1：读节点选择 + 负载上报接受
     * 验证 selectReadNode 在3个节点中能返回非空节点（基本可用性）；
     * 验证 reportNodeLoad 接受负载数据不抛异常（接口幂等性）。
     * 此用例覆盖读路径的基础链路。
     */
    @Test
    void shouldSelectReadableNodeAndAcceptLoadReports() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        DistributionManager distributionManager = new NoopDistributionManager();
        LoadBalancer loadBalancer = new LoadBalancerImpl(metadata, distributionManager);

        List<NodeInfo> nodes = List.of(
                NodeInfo.fromAddress("127.0.0.1:9012"),
                NodeInfo.fromAddress("127.0.0.1:9013"),
                NodeInfo.fromAddress("127.0.0.1:9014")
        );
        NodeInfo selected = loadBalancer.selectReadNode(0, nodes);
        Assertions.assertNotNull(selected);

        loadBalancer.reportNodeLoad("127.0.0.1:9012", new NodeLoad("127.0.0.1:9012", 30, 100, 50));
        loadBalancer.reportNodeLoad("127.0.0.1:9013", new NodeLoad("127.0.0.1:9013", 35, 100, 40));
        loadBalancer.reportNodeLoad("127.0.0.1:9014", new NodeLoad("127.0.0.1:9014", 20, 100, 20));
    }

    /**
     * 场景 T2：加权轮询比例验证（Nginx 平滑加权轮询）
     * 三个节点权重为 Primary=1，Replica1=2，Replica2=2（共5）。
     * 连续选10次，期望命中比约为 2:4:4。
     * 此用例验证 WeightedRoundRobin 算法的平滑性与正确性：
     * Primary 由于写压力已高，读请求仅分配较少比例。
     */
    @Test
    void shouldFollowWeightedRoundRobinRatio() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9012"));
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9013"));
        metadata.alive.add(NodeInfo.fromAddress("127.0.0.1:9014"));
        LoadBalancer loadBalancer = new LoadBalancerImpl(metadata, new NoopDistributionManager());

        List<NodeInfo> nodes = List.of(
                NodeInfo.fromAddress("127.0.0.1:9012"),
                NodeInfo.fromAddress("127.0.0.1:9013"),
                NodeInfo.fromAddress("127.0.0.1:9014")
        );

        Map<String, Integer> hits = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            NodeInfo selected = loadBalancer.selectReadNode(0, nodes);
            hits.merge(selected.getNodeId(), 1, Integer::sum);
        }
        // 权重 1:2:2，10次中 Primary 命中 2 次，两个 Replica 各 4 次
        Assertions.assertEquals(2, hits.getOrDefault("127.0.0.1:9012", 0));
        Assertions.assertEquals(4, hits.getOrDefault("127.0.0.1:9013", 0));
        Assertions.assertEquals(4, hits.getOrDefault("127.0.0.1:9014", 0));
    }

    /**
     * 场景 T3：负载超阈值触发分片迁移 + 路由缓存刷新
     * 向 NodeMonitor 上报两个节点负载：
     *   - n1 = 90%（超过高水位 0.8，触发迁移源）
     *   - n2 = 20%（低于低水位 0.6，作为迁移目标）
     * 验证：
     *   1. MigrationManager 将 partition=0 的 owner 更新为 n2
     *   2. 迁移完成后向 DistributionManager 发出 FULL_REFRESH 事件，路由缓存被清空
     * 此用例验证负载均衡的端到端控制流。
     */
    @Test
    void shouldTriggerMigrationWhenHighAndLowUsageExist() {
        InMemoryMetadata metadata = new InMemoryMetadata();
        NodeInfo n1 = NodeInfo.fromAddress("127.0.0.1:9012");
        NodeInfo n2 = NodeInfo.fromAddress("127.0.0.1:9013");
        metadata.alive.add(n1);
        metadata.alive.add(n2);
        CountingDistributionManager distributionManager = new CountingDistributionManager();
        LoadBalancer loadBalancer = new LoadBalancerImpl(metadata, distributionManager);

        loadBalancer.reportNodeLoad(n1.getNodeId(), new NodeLoad(n1.getNodeId(), 90, 100, 10));
        loadBalancer.reportNodeLoad(n2.getNodeId(), new NodeLoad(n2.getNodeId(), 20, 100, 10));

        // 迁移后 partition=0 的 owner 变为低载节点 n2
        Assertions.assertEquals(0, metadata.updatedPartition);
        Assertions.assertEquals(n2, metadata.updatedPrimary);
        // DistributionManager 收到 FULL_REFRESH 事件，路由缓存被清空
        Assertions.assertEquals(1, distributionManager.refreshCount);
    }

    private static class InMemoryMetadata implements MetadataService {
        private final List<NodeInfo> alive = new ArrayList<>();
        private int updatedPartition = -1;
        private NodeInfo updatedPrimary;

        @Override
        public NodeInfo getPrimaryNode(int partitionId) {
            return null;
        }

        @Override
        public List<NodeInfo> getAllReplicas(int partitionId) {
            return List.of();
        }

        @Override
        public List<NodeInfo> getAllAliveNodes() {
            return new ArrayList<>(alive);
        }

        @Override
        public void updatePartitionOwner(int partitionId, NodeInfo newPrimary) {
            updatedPartition = partitionId;
            updatedPrimary = newPrimary;
        }
    }

    private static class NoopDistributionManager implements DistributionManager {
        @Override
        public NodeInfo routeForWrite(String key) {
            return NodeInfo.fromAddress("127.0.0.1:9012");
        }

        @Override
        public NodeInfo routeForRead(String key) {
            return NodeInfo.fromAddress("127.0.0.1:9012");
        }

        @Override
        public List<NodeInfo> getPartitionNodes(int partitionId) {
            return List.of();
        }

        @Override
        public void onClusterChange(com.zju.minisql.common.cluster.ClusterEvent event) {
            // no-op
        }
    }

    private static class CountingDistributionManager extends NoopDistributionManager {
        private int refreshCount;

        @Override
        public void onClusterChange(com.zju.minisql.common.cluster.ClusterEvent event) {
            if (event.getType() == com.zju.minisql.common.cluster.ClusterEvent.Type.FULL_REFRESH) {
                refreshCount++;
            }
        }
    }
}
