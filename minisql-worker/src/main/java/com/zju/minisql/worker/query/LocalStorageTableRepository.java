package com.zju.minisql.worker.query;

import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.worker.storage.LocalStorageEngine;
import com.zju.minisql.worker.storage.model.ScanOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 基于 LocalStorageEngine 的查询数据仓库。
 */
public class LocalStorageTableRepository implements WorkerTableRepository {

    private static final int PARTITION_BUCKETS = 1024;

    private final LocalStorageEngine storageEngine;

    public LocalStorageTableRepository(LocalStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public List<Row> getTableRows(String tableName) {
        List<Row> rows = new ArrayList<>();
        ScanOptions options = new ScanOptions();
        Iterator<com.zju.minisql.worker.storage.model.Row> iterator = storageEngine.scan(tableName, options);
        while (iterator.hasNext()) {
            com.zju.minisql.worker.storage.model.Row storageRow = iterator.next();
            Row row = new Row();
            for (Map.Entry<String, Object> entry : storageRow.getColumns().entrySet()) {
                row.put(entry.getKey(), entry.getValue());
            }
            rows.add(row);
        }
        return rows;
    }

    @Override
    public void insertRow(String tableName, Row row) {
        String primaryKey = resolvePrimaryKey(row);
        int partitionId = Math.floorMod(primaryKey.hashCode(), PARTITION_BUCKETS);
        Map<String, Object> columns = new HashMap<>(row.getValues());
        com.zju.minisql.worker.storage.model.Row storageRow =
                new com.zju.minisql.worker.storage.model.Row(primaryKey, tableName, partitionId, columns);
        storageEngine.insert(tableName, storageRow);
    }

    @Override
    public void deleteTable(String tableName) {
        storageEngine.deleteTable(tableName);
    }

    private String resolvePrimaryKey(Row row) {
        Object idValue = row.get("id");
        if (idValue != null) {
            return String.valueOf(idValue);
        }
        if (!row.getValues().isEmpty()) {
            return String.valueOf(row.getValues().values().iterator().next());
        }
        return "";
    }
}
