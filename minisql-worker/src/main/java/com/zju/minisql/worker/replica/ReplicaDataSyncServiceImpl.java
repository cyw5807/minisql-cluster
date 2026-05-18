package com.zju.minisql.worker.replica;

import com.zju.minisql.common.replica.ReplicaDataSyncService;
import com.zju.minisql.common.replica.ReplicaSyncAck;
import com.zju.minisql.common.replica.ReplicationLogEntry;
import com.zju.minisql.worker.storage.LocalStorageEngine;
import com.zju.minisql.worker.storage.model.OpType;
import com.zju.minisql.worker.storage.model.ReplicationEntry;
import com.zju.minisql.worker.storage.model.Row;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Worker 侧副本日志同步服务实现。
 */
public class ReplicaDataSyncServiceImpl implements ReplicaDataSyncService {

    private final LocalStorageEngine storageEngine;
    private final ConcurrentMap<Integer, Long> lastAppliedIndex = new ConcurrentHashMap<>();

    public ReplicaDataSyncServiceImpl(LocalStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public ReplicaSyncAck appendEntry(ReplicationLogEntry entry) {
        int partitionId = entry.getPartitionId();
        long expected = getLastAppliedIndex(partitionId) + 1;
        if (entry.getLogIndex() != expected) {
            return ReplicaSyncAck.gap(expected, "log gap detected, expected=" + expected
                    + ", actual=" + entry.getLogIndex());
        }

        apply(entry);
        lastAppliedIndex.put(partitionId, entry.getLogIndex());
        return ReplicaSyncAck.ok(entry.getLogIndex() + 1, "append ok");
    }

    @Override
    public ReplicaSyncAck recoverEntries(int partitionId, List<ReplicationLogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            long expected = getLastAppliedIndex(partitionId) + 1;
            return ReplicaSyncAck.ok(expected, "no recovery entries");
        }

        List<ReplicationLogEntry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparingLong(ReplicationLogEntry::getLogIndex));

        long expected = getLastAppliedIndex(partitionId) + 1;
        for (ReplicationLogEntry entry : ordered) {
            if (entry.getPartitionId() != partitionId) {
                return ReplicaSyncAck.fail("partition mismatch in recovery");
            }
            // 幂等容错：重复日志直接跳过
            if (entry.getLogIndex() < expected) {
                continue;
            }
            if (entry.getLogIndex() > expected) {
                return ReplicaSyncAck.gap(expected, "recovery gap detected, expected=" + expected
                        + ", actual=" + entry.getLogIndex());
            }
            apply(entry);
            expected++;
        }

        lastAppliedIndex.put(partitionId, expected - 1);
        return ReplicaSyncAck.ok(expected, "recovery ok");
    }

    @Override
    public long getLastAppliedIndex(int partitionId) {
        return lastAppliedIndex.getOrDefault(partitionId, 0L);
    }

    private void apply(ReplicationLogEntry logEntry) {
        Map<String, Object> columns = new HashMap<>(logEntry.getRow().getValues());
        String primaryKey = resolvePrimaryKey(logEntry);
        Row storageRow = new Row(primaryKey, logEntry.getTableName(), logEntry.getPartitionId(), columns);
        ReplicationEntry entry = new ReplicationEntry(
                OpType.INSERT,
                logEntry.getTableName(),
                logEntry.getPartitionId(),
                storageRow,
                primaryKey,
                logEntry.getLogIndex(),
                logEntry.getTimestamp()
        );
        storageEngine.applyReplicationLog(entry);
    }

    private String resolvePrimaryKey(ReplicationLogEntry logEntry) {
        if (logEntry.getPrimaryKey() != null && !logEntry.getPrimaryKey().isBlank()) {
            return logEntry.getPrimaryKey();
        }
        Object id = logEntry.getRow().get("id");
        return id == null ? "" : String.valueOf(id);
    }
}
