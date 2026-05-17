package com.zju.minisql.worker.storage;

import com.zju.minisql.worker.storage.model.ReplicationEntry;
import com.zju.minisql.worker.storage.model.Row;
import com.zju.minisql.worker.storage.model.ScanOptions;

import java.util.Iterator;

public interface LocalStorageEngine {

    void insert(String tableName, Row row);

    void delete(String tableName, String primaryKey);

    void update(String tableName, String primaryKey, Row newRow);

    Row get(String tableName, String primaryKey);

    Iterator<Row> scan(String tableName, ScanOptions opts);

    void applyReplicationLog(ReplicationEntry entry);

    byte[] exportPartition(int partitionId);

    void importPartition(int partitionId, byte[] data);
}
