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

    /**
     * 场景：ReplicaSyncRpcTransport 正确委托 SyncClient 的三类 RPC 调用
     * 使用内联 fake SyncClient，验证：
     * 1. syncWrite  → 调用 client.appendEntry，目标节点地址与 logIndex 均正确传递，返回 ok
     * 2. recover    → 调用 client.recoverEntries，entries 列表正确传递，返回 ok
     * 3. getLastAppliedIndex → 调用 client.getLastAppliedIndex，返回值正确透传
     * 此用例验证 Transport 层不篡改参数、不丢弃返回值，保证 RPC 接口契约正确。
     */
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

        // 验证 syncWrite 正确路由并返回 ok
        ReplicaSyncAck writeAck = transport.syncWrite(NodeInfo.fromAddress("127.0.0.1:9013"), entry);
        Assertions.assertTrue(writeAck.isSuccess(), "syncWrite 应返回成功");
        Assertions.assertEquals("127.0.0.1:9013", target.get(), "目标节点地址应正确传递");
        Assertions.assertNotNull(appended.get(), "appendEntry 应被调用");
        Assertions.assertEquals(10L, appended.get().getLogIndex(), "logIndex 应正确透传");

        // 验证 recover 正确路由并返回 ok
        ReplicaSyncAck recoverAck = transport.recover(NodeInfo.fromAddress("127.0.0.1:9013"), 7, List.of(entry));
        Assertions.assertTrue(recoverAck.isSuccess(), "recover 应返回成功");
        Assertions.assertEquals(1, recovered.get().size(), "entries 列表应正确传递");

        // 验证 getLastAppliedIndex 返回值正确透传
        long lastApplied = transport.getLastAppliedIndex(NodeInfo.fromAddress("127.0.0.1:9013"), 7);
        Assertions.assertEquals(9L, lastApplied, "lastAppliedIndex 应正确透传");
    }
}
