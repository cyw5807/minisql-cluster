package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.ClusterEvent;
import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.distribution.DistributionManager;

/**
 * 分片迁移编排器（当前实现为控制面操作，真实数据复制由 Worker 对接）。
 */
public class MigrationManager {

    private final MetadataService metadataService;
    private final DistributionManager distributionManager;

    public MigrationManager(MetadataService metadataService, DistributionManager distributionManager) {
        this.metadataService = metadataService;
        this.distributionManager = distributionManager;
    }

    public void migrate(int partitionId, NodeInfo from, NodeInfo to) {
        copyData(partitionId, from, to);
        switchRoute(partitionId, to);
        cleanOldData(partitionId, from);
    }

    private void copyData(int partitionId, NodeInfo from, NodeInfo to) {
        // 真实环境应触发 Worker 间数据复制任务。
        System.out.println("迁移复制阶段: partition=" + partitionId + ", " + from.address() + " -> " + to.address());
    }

    private void switchRoute(int partitionId, NodeInfo newPrimary) {
        metadataService.updatePartitionOwner(partitionId, newPrimary);
        distributionManager.onClusterChange(ClusterEvent.refresh());
    }

    private void cleanOldData(int partitionId, NodeInfo node) {
        // 真实环境应触发源节点清理任务。
        System.out.println("迁移清理阶段: partition=" + partitionId + ", node=" + node.address());
    }
}
