package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.query.model.Row;

import java.util.List;

/**
 * 主节点写入与副本同步。
 */
public class PrimaryHandler {

    private final ReplicaSyncTransport transport;

    public PrimaryHandler(ReplicaSyncTransport transport) {
        this.transport = transport;
    }

    public boolean handleWrite(int partitionId, Row row, List<NodeInfo> replicas) {
        int ack = 1;
        for (NodeInfo replica : replicas) {
            if (transport.syncWrite(replica, partitionId, row)) {
                ack++;
            }
        }
        int quorum = 1 + Math.max(1, replicas.size() / 2);
        return ack >= quorum;
    }
}
