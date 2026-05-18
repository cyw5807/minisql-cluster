package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;
import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
    private final ConcurrentMap<Integer, Long> partitionLogIndex = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, CopyOnWriteArrayList<ReplicationLogEntry>> partitionLogs = new ConcurrentHashMap<>();

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
        ReplicationLogEntry logEntry = buildLogEntry(partitionId, tableName, row);
        appendLog(logEntry);
        PrimaryHandler.WriteReport report = attemptWrite(logEntry, nodes);
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
            PrimaryHandler.WriteReport retryReport = attemptWrite(logEntry, retriedNodes);
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
        NodeInfo recovered = NodeInfo.fromAddress(nodeId);
        List<Integer> partitionIds = new ArrayList<>(partitionLogs.keySet());
        partitionIds.sort(Comparator.naturalOrder());
        for (Integer partitionId : partitionIds) {
            long lastApplied = primaryHandler.getLastAppliedIndex(recovered, partitionId);
            List<ReplicationLogEntry> missing = loadLogsRange(
                    partitionId,
                    Math.max(1L, lastApplied + 1),
                    partitionLogIndex.getOrDefault(partitionId, 0L)
            );
            if (!missing.isEmpty()) {
                boolean recoveredOk = recoverWithRetry(recovered, partitionId, missing);
                if (recoveredOk) {
                    ensureRecoveredNodeInPartitionCache(partitionId, recovered);
                }
            } else {
                ensureRecoveredNodeInPartitionCache(partitionId, recovered);
            }
        }
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

    private PrimaryHandler.WriteReport attemptWrite(ReplicationLogEntry logEntry, List<NodeInfo> nodes) {
        NodeInfo primary = nodes.get(0);
        List<NodeInfo> replicas = new ArrayList<>(nodes);
        replicas.remove(0);
        return primaryHandler.handleWrite(logEntry, primary, replicas, this::loadLogsRange);
    }

    private ReplicationLogEntry buildLogEntry(int partitionId, String tableName, Row row) {
        long logIndex = partitionLogIndex.merge(partitionId, 1L, Long::sum);
        String primaryKey = extractPrimaryKey(row);
        return new ReplicationLogEntry(
                partitionId,
                tableName,
                row,
                primaryKey,
                logIndex,
                System.currentTimeMillis()
        );
    }

    private String extractPrimaryKey(Row row) {
        Object idValue = row.get("id");
        if (idValue != null) {
            return String.valueOf(idValue);
        }
        if (!row.getValues().isEmpty()) {
            return String.valueOf(row.getValues().values().iterator().next());
        }
        return "";
    }

    private void appendLog(ReplicationLogEntry entry) {
        CopyOnWriteArrayList<ReplicationLogEntry> logs = partitionLogs.computeIfAbsent(
                entry.getPartitionId(), ignored -> new CopyOnWriteArrayList<>()
        );
        logs.add(entry);
    }

    private List<ReplicationLogEntry> loadLogsRange(int partitionId, long fromInclusive, long toInclusive) {
        if (fromInclusive > toInclusive) {
            return List.of();
        }
        List<ReplicationLogEntry> logs = partitionLogs.getOrDefault(partitionId, new CopyOnWriteArrayList<>());
        List<ReplicationLogEntry> range = new ArrayList<>();
        for (ReplicationLogEntry entry : logs) {
            if (entry.getLogIndex() >= fromInclusive && entry.getLogIndex() <= toInclusive) {
                range.add(entry);
            }
        }
        return range;
    }

    private boolean recoverWithRetry(NodeInfo recovered, int partitionId, List<ReplicationLogEntry> missing) {
        int attempts = 0;
        while (attempts++ < 5) {
            if (primaryHandler.recoverReplica(recovered, partitionId, missing)) {
                return true;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void ensureRecoveredNodeInPartitionCache(int partitionId, NodeInfo recovered) {
        List<NodeInfo> nodes = new ArrayList<>(loadPartitionNodes(partitionId));
        boolean exists = nodes.stream().anyMatch(node -> node.getNodeId().equals(recovered.getNodeId()));
        if (!exists) {
            nodes.add(recovered);
            partitionNodes.put(partitionId, nodes);
        }
    }
}
