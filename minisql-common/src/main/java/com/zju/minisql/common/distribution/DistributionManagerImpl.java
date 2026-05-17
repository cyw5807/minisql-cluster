package com.zju.minisql.common.distribution;

import com.zju.minisql.common.cluster.ClusterEvent;
import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据分布模块实现。
 */
public class DistributionManagerImpl implements DistributionManager {

    private static final int PARTITION_BUCKETS = 1024;

    private final ConsistentHashRing hashRing;
    private final RoutingTable routingTable;
    private final MetadataService metadataService;

    public DistributionManagerImpl(MetadataService metadataService) {
        this(metadataService, 150, 30_000L);
    }

    public DistributionManagerImpl(MetadataService metadataService, int virtualNodeCount, long routeTtlMillis) {
        this.metadataService = metadataService;
        this.hashRing = new ConsistentHashRing(virtualNodeCount);
        this.routingTable = new RoutingTable(routeTtlMillis);
        this.hashRing.rebuild(metadataService.getAllAliveNodes());
    }

    @Override
    public NodeInfo routeForWrite(String key) {
        return route(key);
    }

    @Override
    public NodeInfo routeForRead(String key) {
        return route(key);
    }

    @Override
    public List<NodeInfo> getPartitionNodes(int partitionId) {
        NodeInfo primary = metadataService.getPrimaryNode(partitionId);
        List<NodeInfo> replicas = metadataService.getAllReplicas(partitionId);
        List<NodeInfo> nodes = new ArrayList<>();
        if (primary != null) {
            nodes.add(primary);
        }
        nodes.addAll(replicas);
        return nodes;
    }

    @Override
    public void onClusterChange(ClusterEvent event) {
        if (event.getType() == ClusterEvent.Type.NODE_ADDED && event.getNodeInfo() != null) {
            hashRing.addNode(event.getNodeInfo());
        } else if (event.getType() == ClusterEvent.Type.NODE_REMOVED && event.getNodeInfo() != null) {
            hashRing.removeNode(event.getNodeInfo());
        } else {
            hashRing.rebuild(metadataService.getAllAliveNodes());
        }
        routingTable.invalidate();
    }

    private NodeInfo route(String key) {
        int partitionId = partitionIdForKey(key);
        NodeInfo cached = routingTable.get(partitionId);
        if (cached != null) {
            return cached;
        }

        NodeInfo primary = metadataService.getPrimaryNode(partitionId);
        if (primary == null) {
            if (hashRing.isEmpty()) {
                hashRing.rebuild(metadataService.getAllAliveNodes());
            }
            primary = hashRing.getNode(key);
        }
        if (primary == null) {
            throw new IllegalStateException("当前没有可用节点");
        }
        routingTable.put(partitionId, primary);
        return primary;
    }

    private int partitionIdForKey(String key) {
        return Math.floorMod(key.hashCode(), PARTITION_BUCKETS);
    }
}
