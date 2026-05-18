package com.zju.minisql.client.network;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.common.replica.ReplicaSyncAck;
import com.zju.minisql.common.replica.ReplicationLogEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class ReplicaSyncRpcTransportTest {

    @Test
    void shouldForwardAppendAndRecoverCalls() {
        AtomicReference<String> target = new AtomicReference<>();
        AtomicReference<ReplicationLogEntry> appended = new AtomicReference<>();
        AtomicReference<List<ReplicationLogEntry>> recovered = new AtomicReference<>();

        ReplicaSyncRpcTransport.SyncClient fakeClient = new ReplicaSyncRpcTransport.SyncClient() {
            @Override
            public ReplicaSyncAck appendEntry(NodeInfo nodeInfo, ReplicationLogEntry entry) {
                target.set(nodeInfo.address());
                appended.set(entry);
                return ReplicaSyncAck.ok(entry.getLogIndex() + 1, "ok");
            }

            @Override
            public ReplicaSyncAck recoverEntries(NodeInfo nodeInfo, int partitionId, List<ReplicationLogEntry> entries) {
                target.set(nodeInfo.address());
                recovered.set(entries);
                return ReplicaSyncAck.ok(partitionId + 1L, "recover");
            }

            @Override
            public long getLastAppliedIndex(NodeInfo nodeInfo, int partitionId) {
                target.set(nodeInfo.address());
                return 9L;
            }
        };

        ReplicaSyncRpcTransport transport = new ReplicaSyncRpcTransport(fakeClient);
        ReplicationLogEntry entry = new ReplicationLogEntry(
                7,
                "student",
                Row.of("id", 1001, "name", "alice"),
                "1001",
                10L,
                1L
        );

        ReplicaSyncAck writeAck = transport.syncWrite(NodeInfo.fromAddress("127.0.0.1:9013"), entry);
        ReplicaSyncAck recoverAck = transport.recover(
                NodeInfo.fromAddress("127.0.0.1:9013"),
                7,
                List.of(entry)
        );
        long lastApplied = transport.getLastAppliedIndex(NodeInfo.fromAddress("127.0.0.1:9013"), 7);

        Assertions.assertTrue(writeAck.isSuccess());
        Assertions.assertTrue(recoverAck.isSuccess());
        Assertions.assertEquals(9L, lastApplied);
        Assertions.assertEquals("127.0.0.1:9013", target.get());
        Assertions.assertNotNull(appended.get());
        Assertions.assertEquals(10L, appended.get().getLogIndex());
        Assertions.assertEquals(1, recovered.get().size());
    }
}
