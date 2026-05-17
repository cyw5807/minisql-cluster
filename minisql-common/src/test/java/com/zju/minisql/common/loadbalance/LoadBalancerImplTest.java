package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.NodeLoad;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.distribution.DistributionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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

    private static class InMemoryMetadata implements MetadataService {
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
            return new ArrayList<>();
        }

        @Override
        public void updatePartitionOwner(int partitionId, NodeInfo newPrimary) {
            // no-op
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
}
