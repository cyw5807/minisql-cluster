package com.zju.minisql.common.replica;

import com.zju.minisql.common.query.model.Row;

import java.io.Serializable;

/**
 * 副本同步日志条目。
 */
public class ReplicationLogEntry implements Serializable {

    private int partitionId;
    private String tableName;
    private Row row;
    private String primaryKey;
    private long logIndex;
    private long timestamp;

    public ReplicationLogEntry() {
    }

    public ReplicationLogEntry(int partitionId, String tableName, Row row, String primaryKey, long logIndex, long timestamp) {
        this.partitionId = partitionId;
        this.tableName = tableName;
        this.row = row;
        this.primaryKey = primaryKey;
        this.logIndex = logIndex;
        this.timestamp = timestamp;
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
