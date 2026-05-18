package com.zju.minisql.common.distribution;

import com.zju.minisql.common.cluster.ClusterEvent;
import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DistributionManagerImplTest {

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
        Assertions.assertEquals(1, metadata.primaryQueryCount);
    }

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
