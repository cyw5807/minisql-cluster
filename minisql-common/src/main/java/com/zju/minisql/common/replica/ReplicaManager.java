package com.zju.minisql.common.replica;

import com.zju.minisql.common.query.model.Row;

public interface ReplicaManager {

    ReplicaResult write(int partitionId, String tableName, Row row);

    ReplicaResult read(int partitionId, String key);

    void onNodeFailure(String nodeId);

    void onNodeRecovery(String nodeId);
}
