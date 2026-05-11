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

    private static class InMemoryMetadata implements MetadataService {
        private final List<NodeInfo> alive = new ArrayList<>();

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
            // no-op
        }
    }
}
