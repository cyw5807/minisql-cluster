package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;

import java.util.List;

/**
 * 副本同步传输抽象，默认由上层接入 RPC。
 */
public interface ReplicaSyncTransport {

    ReplicaSyncAck syncWrite(NodeInfo nodeInfo, ReplicationLogEntry entry);

    ReplicaSyncAck recover(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries);

    long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId);
}
