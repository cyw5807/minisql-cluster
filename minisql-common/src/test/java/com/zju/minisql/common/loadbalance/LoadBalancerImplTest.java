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
        Assertions.assertEquals(2, hits.getOrDefault("127.0.0.1:9012", 0));
        Assertions.assertEquals(4, hits.getOrDefault("127.0.0.1:9013", 0));
        Assertions.assertEquals(4, hits.getOrDefault("127.0.0.1:9014", 0));
    }

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

        Assertions.assertEquals(0, metadata.updatedPartition);
        Assertions.assertEquals(n2, metadata.updatedPrimary);
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
