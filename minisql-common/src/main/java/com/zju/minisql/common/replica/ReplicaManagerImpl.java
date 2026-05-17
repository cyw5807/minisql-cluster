package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 副本管理模块实现。
 */
public class ReplicaManagerImpl implements ReplicaManager {

    private final MetadataService metadataService;
    private final PrimaryHandler primaryHandler;
    private final ReplicaHandler replicaHandler;
    private final FailoverHandler failoverHandler;

    private final ConcurrentMap<Integer, List<NodeInfo>> partitionNodes = new ConcurrentHashMap<>();

    public ReplicaManagerImpl(MetadataService metadataService,
                              PrimaryHandler primaryHandler,
                              ReplicaHandler replicaHandler,
                              FailoverHandler failoverHandler) {
        this.metadataService = metadataService;
        this.primaryHandler = primaryHandler;
        this.replicaHandler = replicaHandler;
        this.failoverHandler = failoverHandler;
    }

    @Override
    public ReplicaResult write(int partitionId, String tableName, Row row) {
        List<NodeInfo> nodes = loadPartitionNodes(partitionId);
        if (nodes.isEmpty()) {
            return ReplicaResult.fail("分片没有可用节点: " + partitionId);
        }
        NodeInfo primary = nodes.get(0);
        PrimaryHandler.WriteReport report = attemptWrite(partitionId, tableName, row, nodes);
        if (report.isQuorumMet()) {
            return ReplicaResult.ok("写入成功");
        }

        // primary 不可写时，自动触发一次故障切换并重试
        if (!report.isPrimaryWriteOk()) {
            onNodeFailure(primary.getNodeId());
            List<NodeInfo> retriedNodes = loadPartitionNodes(partitionId);
            if (retriedNodes.isEmpty() || retriedNodes.get(0).getNodeId().equals(primary.getNodeId())) {
                return ReplicaResult.fail("primary 写入失败，且故障切换未成功");
            }
            PrimaryHandler.WriteReport retryReport = attemptWrite(partitionId, tableName, row, retriedNodes);
            if (retryReport.isQuorumMet()) {
                return ReplicaResult.ok("primary 故障切换后写入成功");
            }
            if (!retryReport.isPrimaryWriteOk()) {
                return ReplicaResult.fail("新 primary 写入失败");
            }
            return ReplicaResult.fail("故障切换后副本确认数不足: ack=" + retryReport.getAck()
                    + ", quorum=" + retryReport.getQuorum());
        }
        return ReplicaResult.fail("副本确认数不足: ack=" + report.getAck() + ", quorum=" + report.getQuorum());
    }

    @Override
    public ReplicaResult read(int partitionId, String key) {
        List<NodeInfo> nodes = loadPartitionNodes(partitionId);
        if (nodes.isEmpty()) {
            return ReplicaResult.fail("分片没有可用节点: " + partitionId);
        }
        NodeInfo selected = replicaHandler.selectReadNode(partitionId, nodes);
        return ReplicaResult.ok("读取路由到: " + selected.address() + ", key=" + key);
    }

    @Override
    public void onNodeFailure(String nodeId) {
        for (Integer partitionId : partitionNodes.keySet()) {
            List<NodeInfo> nodes = new ArrayList<>(loadPartitionNodes(partitionId));
            if (nodes.isEmpty()) {
                continue;
            }
            NodeInfo primary = nodes.get(0);
            if (!primary.getNodeId().equals(nodeId)) {
                continue;
            }
            List<NodeInfo> replicas = new ArrayList<>(nodes);
            replicas.remove(0);
            replicas.removeIf(replica -> replica.getNodeId().equals(nodeId));
            NodeInfo newPrimary = failoverHandler.electNewPrimary(partitionId, replicas);
            if (newPrimary != null) {
                replicas.remove(newPrimary);
                List<NodeInfo> updated = new ArrayList<>();
                updated.add(newPrimary);
                updated.addAll(replicas);
                partitionNodes.put(partitionId, updated);
            }
        }
    }

    @Override
    public void onNodeRecovery(String nodeId) {
        partitionNodes.clear();
    }

    private List<NodeInfo> loadPartitionNodes(int partitionId) {
        return partitionNodes.computeIfAbsent(partitionId, id -> {
            List<NodeInfo> nodes = new ArrayList<>();
            NodeInfo primary = metadataService.getPrimaryNode(id);
            if (primary != null) {
                nodes.add(primary);
            }
            nodes.addAll(metadataService.getAllReplicas(id));
            return nodes;
        });
    }

    private PrimaryHandler.WriteReport attemptWrite(int partitionId, String tableName, Row row, List<NodeInfo> nodes) {
        NodeInfo primary = nodes.get(0);
        List<NodeInfo> replicas = new ArrayList<>(nodes);
        replicas.remove(0);
        return primaryHandler.handleWrite(partitionId, tableName, row, primary, replicas);
    }
}
