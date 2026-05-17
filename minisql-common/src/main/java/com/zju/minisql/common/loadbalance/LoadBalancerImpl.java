package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.NodeLoad;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.distribution.DistributionManager;

import java.util.List;

/**
 * 负载均衡模块实现。
 */
public class LoadBalancerImpl implements LoadBalancer {

    private final WeightedRoundRobin weightedRoundRobin;
    private final NodeMonitor nodeMonitor;

    public LoadBalancerImpl(MetadataService metadataService, DistributionManager distributionManager) {
        this.weightedRoundRobin = new WeightedRoundRobin();
        MigrationManager migrationManager = new MigrationManager(metadataService, distributionManager);
        this.nodeMonitor = new NodeMonitor(0.8D, 0.6D, metadataService, migrationManager);
    }

    @Override
    public NodeInfo selectReadNode(int partitionId, List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalStateException("partition " + partitionId + " 没有可用副本");
        }
        // 约定 nodes[0] 为 primary，其余是 replica。
        for (int i = 0; i < nodes.size(); i++) {
            weightedRoundRobin.updateWeight(nodes.get(i).getNodeId(), i == 0 ? 1 : 2);
        }
        return weightedRoundRobin.select(nodes);
    }

    @Override
    public void reportNodeLoad(String nodeId, NodeLoad load) {
        nodeMonitor.report(load);
    }
}
