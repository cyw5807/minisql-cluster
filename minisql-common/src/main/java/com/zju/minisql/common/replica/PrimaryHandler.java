package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;

import java.util.Collections;
import java.util.List;

/**
 * 主节点写入与副本同步。
 */
public class PrimaryHandler {

    private final ReplicaSyncTransport transport;

    public PrimaryHandler(ReplicaSyncTransport transport) {
        this.transport = transport;
    }

    public WriteReport handleWrite(ReplicationLogEntry entry,
                                   NodeInfo primary,
                                   List<NodeInfo> replicas,
                                   MissingLogProvider missingLogProvider) {
        boolean primaryWriteOk = syncWithRecovery(primary, entry, missingLogProvider);
        if (!primaryWriteOk) {
            return new WriteReport(false, false, 0, 1 + Math.max(1, replicas.size() / 2));
        }

        int ack = 1;
        for (NodeInfo replica : replicas) {
            if (syncWithRecovery(replica, entry, missingLogProvider)) {
                ack++;
            }
        }
        int quorum = 1 + Math.max(1, replicas.size() / 2);
        return new WriteReport(true, ack >= quorum, ack, quorum);
    }

    private boolean syncWithRecovery(NodeInfo nodeInfo,
                                     ReplicationLogEntry entry,
                                     MissingLogProvider missingLogProvider) {
        ReplicaSyncAck ack = transport.syncWrite(nodeInfo, entry);
        int attempts = 0;
        while (!ack.isSuccess() && ack.isGapDetected() && attempts++ < 3) {
            long expected = ack.getExpectedLogIndex();
            if (expected < 1 || expected > entry.getLogIndex()) {
                return false;
            }
            if (expected < entry.getLogIndex()) {
                List<ReplicationLogEntry> missing = missingLogProvider.loadMissingLogs(
                        entry.getPartitionId(), expected, entry.getLogIndex() - 1
                );
                if (missing == null) {
                    missing = Collections.emptyList();
                }
                if (!missing.isEmpty()) {
                    ReplicaSyncAck recoverAck = transport.recover(nodeInfo, entry.getPartitionId(), missing);
                    if (!recoverAck.isSuccess()) {
                        return false;
                    }
                }
            }
            ack = transport.syncWrite(nodeInfo, entry);
        }
        return ack.isSuccess();
    }

    public boolean recoverReplica(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
        ReplicaSyncAck ack = transport.recover(nodeInfo, partitionId, entries);
        return ack.isSuccess();
    }

    @FunctionalInterface
    public interface MissingLogProvider {
        List<ReplicationLogEntry> loadMissingLogs(int partitionId, long fromInclusive, long toInclusive);
    }

    public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
        return transport.getLastAppliedIndex(nodeInfo, partitionId);
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
