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

    public WriteReport handleWrite(int partitionId, String tableName, Row row, NodeInfo primary, List<NodeInfo> replicas) {
        boolean primaryWriteOk = transport.syncWrite(primary, partitionId, tableName, row);
        if (!primaryWriteOk) {
            return new WriteReport(false, false, 0, 1 + Math.max(1, replicas.size() / 2));
        }

        int ack = 1;
        for (NodeInfo replica : replicas) {
            if (transport.syncWrite(replica, partitionId, tableName, row)) {
                ack++;
            }
        }
        int quorum = 1 + Math.max(1, replicas.size() / 2);
        return new WriteReport(true, ack >= quorum, ack, quorum);
    }

    public static class WriteReport {
        private final boolean primaryWriteOk;
        private final boolean quorumMet;
        private final int ack;
        private final int quorum;

        public WriteReport(boolean primaryWriteOk, boolean quorumMet, int ack, int quorum) {
            this.primaryWriteOk = primaryWriteOk;
            this.quorumMet = quorumMet;
            this.ack = ack;
            this.quorum = quorum;
        }

        public boolean isPrimaryWriteOk() {
            return primaryWriteOk;
        }

        public boolean isQuorumMet() {
            return quorumMet;
        }

        public int getAck() {
            return ack;
        }

        public int getQuorum() {
            return quorum;
        }
    }
}
