package com.zju.minisql.client.network;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.replica.ReplicaDataSyncService;
import com.zju.minisql.common.replica.ReplicaSyncAck;
import com.zju.minisql.common.replica.ReplicaSyncTransport;
import com.zju.minisql.common.replica.ReplicationLogEntry;
import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.client.NettyRpcClient;

import java.util.List;

/**
 * 基于 RPC 的副本同步传输实现。
 */
public class ReplicaSyncRpcTransport implements ReplicaSyncTransport {

    private final SyncClient syncClient;

    public ReplicaSyncRpcTransport() {
        this(new NettySyncClient());
    }

    ReplicaSyncRpcTransport(SyncClient syncClient) {
        this.syncClient = syncClient;
    }

    @Override
    public ReplicaSyncAck syncWrite(NodeInfo nodeInfo, ReplicationLogEntry entry) {
        return syncClient.appendEntry(nodeInfo, entry);
    }

    @Override
    public ReplicaSyncAck recover(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
        return syncClient.recoverEntries(nodeInfo, partitionId, entries);
    }

    @Override
    public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
        return syncClient.getLastAppliedIndex(nodeInfo, partitionId);
    }

    interface SyncClient {
        ReplicaSyncAck appendEntry(NodeInfo nodeInfo, ReplicationLogEntry entry);

        ReplicaSyncAck recoverEntries(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries);

        long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId);
    }

    private static final class NettySyncClient implements SyncClient {
        private final NettyRpcClient rpcClient = new NettyRpcClient();

        @Override
        public ReplicaSyncAck appendEntry(NodeInfo nodeInfo, ReplicationLogEntry entry) {
            RpcRequest request = new RpcRequest(
                    ReplicaDataSyncService.class.getCanonicalName(),
                    "appendEntry",
                    new Class[]{ReplicationLogEntry.class},
                    new Object[]{entry}
            );
            return invoke(nodeInfo, request);
        }

        @Override
        public ReplicaSyncAck recoverEntries(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
            RpcRequest request = new RpcRequest(
                    ReplicaDataSyncService.class.getCanonicalName(),
                    "recoverEntries",
                    new Class[]{int.class, List.class},
                    new Object[]{partitionId, entries}
            );
            return invoke(nodeInfo, request);
        }

        @Override
        public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
            RpcRequest request = new RpcRequest(
                    ReplicaDataSyncService.class.getCanonicalName(),
                    "getLastAppliedIndex",
                    new Class[]{int.class},
                    new Object[]{partitionId}
            );
            String[] hostAndPort = nodeInfo.address().split(":");
            String ip = hostAndPort[0];
            int port = Integer.parseInt(hostAndPort[1]);
            try {
                RpcResponse response = rpcClient.sendRequestSync(request, ip, port);
                if (response.getErrorMessage() != null) {
                    return 0L;
                }
                Object result = response.getResult();
                if (result instanceof Number) {
                    return ((Number) result).longValue();
                }
                return 0L;
            } catch (Exception e) {
                return 0L;
            }
        }

        private ReplicaSyncAck invoke(NodeInfo nodeInfo, RpcRequest request) {
            String[] hostAndPort = nodeInfo.address().split(":");
            String ip = hostAndPort[0];
            int port = Integer.parseInt(hostAndPort[1]);
        try {
                RpcResponse response = rpcClient.sendRequestSync(request, ip, port);
                if (response.getErrorMessage() != null) {
                    return ReplicaSyncAck.fail(response.getErrorMessage());
                }
                return (ReplicaSyncAck) response.getResult();
            } catch (Exception e) {
                return ReplicaSyncAck.fail("rpc error: " + e.getMessage());
            }
        }
    }
}
