package com.zju.minisql.worker.storage;

import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import com.zju.minisql.worker.storage.model.OpType;
import com.zju.minisql.worker.storage.model.ReplicationEntry;
import com.zju.minisql.worker.storage.model.Row;
import com.zju.minisql.worker.storage.model.ScanOptions;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * 本地存储引擎实现（基于 MVStore）。
 */
public class LocalStorageEngineImpl implements LocalStorageEngine {

    private final PartitionManager partitionManager;
    private final PersistentOperationLogger operationLogger;
    private final KryoSerializer serializer = new KryoSerializer();

    public LocalStorageEngineImpl(Path dataDir) {
        this.partitionManager = new PartitionManager(dataDir);
        this.operationLogger = new PersistentOperationLogger(dataDir);
    }

    @Override
    public void insert(String tableName, Row row) {
        insertInternal(tableName, row);
        operationLogger.logCrud("INSERT", tableName, row.getPrimaryKey(), row.getPartitionId(), "local write");
        operationLogger.logEntry("LOCAL", buildLocalEntry(OpType.INSERT, tableName, row.getPartitionId(), row, row.getPrimaryKey()));
    }

    @Override
    public void delete(String tableName, String primaryKey) {
        int deletedCount = deleteInternal(tableName, primaryKey);
        operationLogger.logCrud("DELETE", tableName, primaryKey, null, "deletedRows=" + deletedCount);
        Row emptyRow = new Row(primaryKey, tableName, -1, new HashMap<>());
        operationLogger.logEntry("LOCAL", buildLocalEntry(OpType.DELETE, tableName, -1, emptyRow, primaryKey));
    }

    @Override
    public void update(String tableName, String primaryKey, Row newRow) {
        deleteInternal(tableName, primaryKey);
        insertInternal(tableName, newRow);
        operationLogger.logCrud("UPDATE", tableName, primaryKey, newRow.getPartitionId(), "local update");
        operationLogger.logEntry("LOCAL", buildLocalEntry(OpType.UPDATE, tableName, newRow.getPartitionId(), newRow, primaryKey));
    }

    @Override
    public Row get(String tableName, String primaryKey) {
        String key = scopedKey(tableName, primaryKey);
        for (Map.Entry<Integer, Map<String, byte[]>> partition : partitionManager.stores().entrySet()) {
            byte[] data = partition.getValue().get(key);
            if (data != null) {
                Row row = serializer.deserialize(data, Row.class);
                operationLogger.logCrud("READ_POINT", tableName, primaryKey, row.getPartitionId(), "hit");
                return row;
            }
        }
        operationLogger.logCrud("READ_POINT", tableName, primaryKey, null, "miss");
        return null;
    }

    @Override
    public Iterator<Row> scan(String tableName, ScanOptions opts) {
        List<Row> rows = new ArrayList<>();
        String scopedStart = opts.getStartKey() == null ? null : scopedKey(tableName, opts.getStartKey());
        String scopedEnd = opts.getEndKey() == null ? null : scopedKey(tableName, opts.getEndKey());
        Predicate<Row> predicate = opts.getPredicate() == null ? row -> true : opts.getPredicate();

        for (Map<String, byte[]> store : partitionManager.stores().values()) {
            TreeMap<String, byte[]> sorted = new TreeMap<>(store);
            for (Map.Entry<String, byte[]> entry : sorted.entrySet()) {
                if (!entry.getKey().startsWith(tableName + "::")) {
                    continue;
                }
                if (scopedStart != null && entry.getKey().compareTo(scopedStart) < 0) {
                    continue;
                }
                if (scopedEnd != null && entry.getKey().compareTo(scopedEnd) > 0) {
                    continue;
                }
                Row row = serializer.deserialize(entry.getValue(), Row.class);
                if (predicate.test(row)) {
                    rows.add(row);
                }
            }
        }

        int from = Math.min(opts.getOffset(), rows.size());
        int to = opts.getLimit() < 0 ? rows.size() : Math.min(from + opts.getLimit(), rows.size());
        List<Row> page = rows.subList(from, to);
        operationLogger.logCrud("READ_SCAN", tableName, "", null, "matched=" + rows.size() + ", returned=" + page.size());
        return page.iterator();
    }

    @Override
    public void deleteTable(String tableName) {
        String prefix = tableName + "::";
        int removed = 0;
        for (Map.Entry<Integer, Map<String, byte[]>> partition : partitionManager.stores().entrySet()) {
            Map<String, byte[]> store = partition.getValue();
            List<String> toRemove = new ArrayList<>();
            for (String key : store.keySet()) {
                if (key.startsWith(prefix)) {
                    toRemove.add(key);
                }
            }
            for (String key : toRemove) {
                store.remove(key);
            }
            removed += toRemove.size();
            boolean changed = !toRemove.isEmpty();
            if (changed) {
                partitionManager.flush(partition.getKey());
            }
        }
        operationLogger.logCrud("DROP_TABLE", tableName, "", null, "removedRows=" + removed);
    }

    @Override
    public void applyReplicationLog(ReplicationEntry entry) {
        operationLogger.logEntry("REPLICA", entry);
        if (entry.getType() == OpType.DELETE) {
            int deleted = deleteInternal(entry.getRow().getTableName(), entry.getPrimaryKey());
            operationLogger.logCrud(
                    "DELETE",
                    entry.getRow().getTableName(),
                    entry.getPrimaryKey(),
                    entry.getPartitionId(),
                    "replica,deletedRows=" + deleted + ",logIndex=" + entry.getLogIndex()
            );
            return;
        }
        if (entry.getType() == OpType.UPDATE) {
            deleteInternal(entry.getRow().getTableName(), entry.getPrimaryKey());
            insertInternal(entry.getRow().getTableName(), entry.getRow());
            operationLogger.logCrud(
                    "UPDATE",
                    entry.getRow().getTableName(),
                    entry.getPrimaryKey(),
                    entry.getPartitionId(),
                    "replica,logIndex=" + entry.getLogIndex()
            );
            return;
        }
        insertInternal(entry.getRow().getTableName(), entry.getRow());
        operationLogger.logCrud(
                "INSERT",
                entry.getRow().getTableName(),
                entry.getPrimaryKey(),
                entry.getPartitionId(),
                "replica,logIndex=" + entry.getLogIndex()
        );
    }

    @Override
    public byte[] exportPartition(int partitionId) {
        return partitionManager.exportPartition(partitionId);
    }

    @Override
    public void importPartition(int partitionId, byte[] data) {
        partitionManager.importPartition(partitionId, data);
    }

    private String scopedKey(String tableName, String primaryKey) {
        return tableName + "::" + primaryKey;
    }

    private void insertInternal(String tableName, Row row) {
        String scopedKey = scopedKey(tableName, row.getPrimaryKey());
        Map<String, byte[]> store = partitionManager.getStore(row.getPartitionId());
        store.put(scopedKey, serializer.serialize(row));
        partitionManager.flush(row.getPartitionId());
    }

    private int deleteInternal(String tableName, String primaryKey) {
        int deletedCount = 0;
        for (Map.Entry<Integer, Map<String, byte[]>> partition : partitionManager.stores().entrySet()) {
            byte[] removed = partition.getValue().remove(scopedKey(tableName, primaryKey));
            if (removed != null) {
                deletedCount++;
                partitionManager.flush(partition.getKey());
            }
        }
        return deletedCount;
    }

    private ReplicationEntry buildLocalEntry(OpType type,
                                             String tableName,
                                             int partitionId,
                                             Row row,
                                             String primaryKey) {
        return new ReplicationEntry(
                type,
                tableName,
                partitionId,
                row,
                primaryKey,
                -1L,
                System.currentTimeMillis()
        );
    }
}
