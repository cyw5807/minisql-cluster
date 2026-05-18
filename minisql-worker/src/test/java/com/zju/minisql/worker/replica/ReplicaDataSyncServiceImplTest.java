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

    @Test
    void shouldDetectGapAndRecoverMissingLogs() {
        LocalStorageEngine storageEngine = new LocalStorageEngineImpl(tempDir);
        ReplicaDataSyncServiceImpl service = new ReplicaDataSyncServiceImpl(storageEngine);

        ReplicationLogEntry second = new ReplicationLogEntry(1, "student", Row.of("id", 2, "name", "b"), "2", 2L, 2L);
        ReplicaSyncAck firstAck = service.appendEntry(second);
        Assertions.assertTrue(firstAck.isGapDetected());
        Assertions.assertEquals(1L, firstAck.getExpectedLogIndex());

        ReplicationLogEntry first = new ReplicationLogEntry(1, "student", Row.of("id", 1, "name", "a"), "1", 1L, 1L);
        ReplicaSyncAck recoverAck = service.recoverEntries(1, List.of(first));
        Assertions.assertTrue(recoverAck.isSuccess());
        Assertions.assertEquals(2L, service.getLastAppliedIndex(1) + 1);

        ReplicaSyncAck appendAck = service.appendEntry(second);
        Assertions.assertTrue(appendAck.isSuccess());
        Assertions.assertEquals(2L, service.getLastAppliedIndex(1));
    }
}
