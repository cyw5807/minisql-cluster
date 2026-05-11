package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.NodeLoad;
import com.zju.minisql.common.cluster.meta.MetadataService;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点负载监控，达到阈值时触发迁移。
 */
public class NodeMonitor {

    private final double highThreshold;
    private final double lowThreshold;
    private final MetadataService metadataService;
    private final MigrationManager migrationManager;
    private final Map<String, NodeLoad> latestLoad = new ConcurrentHashMap<>();

    public NodeMonitor(double highThreshold,
                       double lowThreshold,
                       MetadataService metadataService,
                       MigrationManager migrationManager) {
        this.highThreshold = highThreshold;
        this.lowThreshold = lowThreshold;
        this.metadataService = metadataService;
        this.migrationManager = migrationManager;
    }

    public void report(NodeLoad load) {
        latestLoad.put(load.getNodeId(), load);
        checkAndTriggerMigration();
    }

    private void checkAndTriggerMigration() {
        NodeLoad overloaded = latestLoad.values().stream()
                .filter(load -> load.usage() > highThreshold)
                .max(Comparator.comparingDouble(NodeLoad::usage))
                .orElse(null);
        if (overloaded == null) {
            return;
        }

        NodeLoad underloaded = latestLoad.values().stream()
                .filter(load -> load.usage() < lowThreshold)
                .min(Comparator.comparingDouble(NodeLoad::usage))
                .orElse(null);
        if (underloaded == null) {
            return;
        }

        NodeInfo from = metadataService.getAllAliveNodes().stream()
                .filter(node -> node.getNodeId().equals(overloaded.getNodeId()))
                .findFirst()
                .orElse(null);
        NodeInfo to = metadataService.getAllAliveNodes().stream()
                .filter(node -> node.getNodeId().equals(underloaded.getNodeId()))
                .findFirst()
                .orElse(null);
        if (from == null || to == null) {
            return;
        }
        // 当前缺少真实分片统计，先以默认分片 0 示意迁移流程。
        migrationManager.migrate(0, from, to);
    }
}
