package com.zju.minisql.worker.storage;

import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import com.zju.minisql.worker.storage.model.OpType;
import com.zju.minisql.worker.storage.model.ReplicationEntry;
import com.zju.minisql.worker.storage.model.Row;
import com.zju.minisql.worker.storage.model.ScanOptions;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * 本地存储引擎实现（轻量版）。
 */
public class LocalStorageEngineImpl implements LocalStorageEngine {

    private final PartitionManager partitionManager;
    private final KryoSerializer serializer = new KryoSerializer();

    public LocalStorageEngineImpl(Path dataDir) {
        this.partitionManager = new PartitionManager(dataDir);
    }

    @Override
    public void insert(String tableName, Row row) {
        String scopedKey = scopedKey(tableName, row.getPrimaryKey());
        Map<String, byte[]> store = partitionManager.getStore(row.getPartitionId());
        store.put(scopedKey, serializer.serialize(row));
        partitionManager.flush(row.getPartitionId());
    }

    @Override
    public void delete(String tableName, String primaryKey) {
        for (Map.Entry<Integer, ConcurrentHashMap<String, byte[]>> partition : partitionManager.stores().entrySet()) {
            partition.getValue().remove(scopedKey(tableName, primaryKey));
            partitionManager.flush(partition.getKey());
        }
    }

    @Override
    public void update(String tableName, String primaryKey, Row newRow) {
        delete(tableName, primaryKey);
        insert(tableName, newRow);
    }

    @Override
    public Row get(String tableName, String primaryKey) {
        String key = scopedKey(tableName, primaryKey);
        for (Map.Entry<Integer, ConcurrentHashMap<String, byte[]>> partition : partitionManager.stores().entrySet()) {
            byte[] data = partition.getValue().get(key);
            if (data != null) {
                return serializer.deserialize(data, Row.class);
            }
        }
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
        return rows.subList(from, to).iterator();
    }

    @Override
    public void applyReplicationLog(ReplicationEntry entry) {
        if (entry.getType() == OpType.DELETE) {
            delete(entry.getRow().getTableName(), entry.getPrimaryKey());
            return;
        }
        if (entry.getType() == OpType.UPDATE) {
            update(entry.getRow().getTableName(), entry.getPrimaryKey(), entry.getRow());
            return;
        }
        insert(entry.getRow().getTableName(), entry.getRow());
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
}
