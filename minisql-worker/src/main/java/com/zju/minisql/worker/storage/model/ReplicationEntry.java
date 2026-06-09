package com.zju.minisql.worker.storage.model;

public class ReplicationEntry {

    private OpType type;
    private String tableName;
    private int partitionId;
    private Row row;
    private String primaryKey;
    private long logIndex;
    private long timestamp;

    public ReplicationEntry(OpType type, String tableName, int partitionId, Row row, String primaryKey, long logIndex, long timestamp) {
        this.type = type;
        this.tableName = tableName;
        this.partitionId = partitionId;
        this.row = row;
        this.primaryKey = primaryKey;
        this.logIndex = logIndex;
        this.timestamp = timestamp;
    }

    public OpType getType() {
        return type;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getTableName() {
        return tableName;
    }

    public Row getRow() {
        return row;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
