package com.zju.minisql.common.replica;

import java.util.List;

/**
 * 副本同步 RPC 服务（由 Worker 提供）。
 */
public interface ReplicaDataSyncService {

    ReplicaSyncAck appendEntry(ReplicationLogEntry entry);

    ReplicaSyncAck recoverEntries(int partitionId, List<ReplicationLogEntry> entries);

    long getLastAppliedIndex(int partitionId);
}
