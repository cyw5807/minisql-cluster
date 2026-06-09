package com.zju.minisql.worker.replica;

import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.common.replica.ReplicaSyncAck;
import com.zju.minisql.common.replica.ReplicationLogEntry;
import com.zju.minisql.worker.storage.LocalStorageEngine;
import com.zju.minisql.worker.storage.LocalStorageEngineImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

class ReplicaDataSyncServiceImplTest {

    @TempDir
    Path tempDir;

    /**
     * 场景 T1：Worker 侧 logIndex 不连续时的 Gap 检测 + recoverEntries 补偿
     * 步骤：
     * 1. 直接发送 logIndex=2 的条目（跳过了 logIndex=1），
     *    appendEntry 应返回 gap，expectedLogIndex=1，说明 Worker 检测到缺口。
     * 2. 通过 recoverEntries 补发 logIndex=1 的条目，
     *    验证回放成功，lastAppliedIndex 推进到 1。
     * 3. 再次调用 appendEntry(logIndex=2)，此时连续性恢复，
     *    验证写入成功，lastAppliedIndex 推进到 2。
     * 覆盖副本被动 gap 检测的完整修复链路。
     */
    @Test
    void shouldDetectGapAndRecoverMissingLogs() {
        LocalStorageEngine storageEngine = new LocalStorageEngineImpl(tempDir);
        ReplicaDataSyncServiceImpl service = new ReplicaDataSyncServiceImpl(storageEngine);

        // 第一步：发送 logIndex=2，期望返回 gap(expectedLogIndex=1)
        ReplicationLogEntry second = new ReplicationLogEntry(1, "student", Row.of("id", 2, "name", "b"), "2", 2L, 2L);
        ReplicaSyncAck firstAck = service.appendEntry(second);
        Assertions.assertTrue(firstAck.isGapDetected(), "应检测到 gap");
        Assertions.assertEquals(1L, firstAck.getExpectedLogIndex(), "期望下一条 logIndex 为 1");

        // 第二步：补发缺失的 logIndex=1
        ReplicationLogEntry first = new ReplicationLogEntry(1, "student", Row.of("id", 1, "name", "a"), "1", 1L, 1L);
        ReplicaSyncAck recoverAck = service.recoverEntries(1, List.of(first));
        Assertions.assertTrue(recoverAck.isSuccess(), "recoverEntries 应成功");
        Assertions.assertEquals(2L, service.getLastAppliedIndex(1) + 1, "恢复后下一期望 logIndex 应为 2");

        // 第三步：重新发送 logIndex=2，此时连续性恢复，应写入成功
        ReplicaSyncAck appendAck = service.appendEntry(second);
        Assertions.assertTrue(appendAck.isSuccess(), "补齐后 appendEntry(logIndex=2) 应成功");
        Assertions.assertEquals(2L, service.getLastAppliedIndex(1), "最终 lastAppliedIndex 应为 2");
    }

    /**
     * 场景 T2：recoverEntries 幂等回放（重复发送同一 logIndex 不重复写入）
     * 步骤：
     * 1. 通过 recoverEntries 回放 logIndex=1 的条目，写入成功。
     * 2. 再次调用 recoverEntries 传入相同的 logIndex=1，
     *    验证接口幂等：不抛异常、不重复写入，lastAppliedIndex 仍为 1。
     * 覆盖网络重传场景下副本不会因重复回放产生数据错误。
     */
    @Test
    void shouldApplyEntriesIdempotently() {
        LocalStorageEngine storageEngine = new LocalStorageEngineImpl(tempDir);
        ReplicaDataSyncServiceImpl service = new ReplicaDataSyncServiceImpl(storageEngine);

        ReplicationLogEntry entry = new ReplicationLogEntry(1, "student", Row.of("id", 1, "name", "a"), "1", 1L, 1L);

        // 第一次回放
        ReplicaSyncAck firstAck = service.recoverEntries(1, List.of(entry));
        Assertions.assertTrue(firstAck.isSuccess(), "首次 recoverEntries 应成功");
        Assertions.assertEquals(1L, service.getLastAppliedIndex(1));

        // 第二次传入相同条目，应幂等忽略，不改变状态
        ReplicaSyncAck secondAck = service.recoverEntries(1, List.of(entry));
        Assertions.assertTrue(secondAck.isSuccess(), "重复 recoverEntries 应幂等成功");
        Assertions.assertEquals(1L, service.getLastAppliedIndex(1), "幂等后 lastAppliedIndex 不变");
    }
}
