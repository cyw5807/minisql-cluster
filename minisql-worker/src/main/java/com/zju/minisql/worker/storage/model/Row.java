package com.zju.minisql.worker.storage.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Row implements Serializable {

    private String primaryKey;
    private String tableName;
    private int partitionId;
    private Map<String, Object> columns = new HashMap<>();

    public Row() {
    }

    public Row(String primaryKey, String tableName, int partitionId, Map<String, Object> columns) {
        this.primaryKey = primaryKey;
        this.tableName = tableName;
        this.partitionId = partitionId;
        this.columns = new HashMap<>(columns);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getTableName() {
        return tableName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Map<String, Object> getColumns() {
        return columns;
    }
}
