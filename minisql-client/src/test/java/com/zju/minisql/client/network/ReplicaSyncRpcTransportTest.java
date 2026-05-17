package com.zju.minisql.client.network;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.common.query.model.TaskFragment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

class ReplicaSyncRpcTransportTest {

    @Test
    void shouldBuildInsertFragmentAndSendToReplica() {
        AtomicReference<String> target = new AtomicReference<>();
        AtomicReference<TaskFragment> captured = new AtomicReference<>();

        FragmentTaskClient fakeClient = (workerAddress, fragment) -> {
            target.set(workerAddress);
            captured.set(fragment);
            return PartialQueryResult.forRows(workerAddress, Collections.emptyList());
        };

        ReplicaSyncRpcTransport transport = new ReplicaSyncRpcTransport(fakeClient);
        Row row = Row.of("id", 1001, "name", "alice");

        boolean success = transport.syncWrite(NodeInfo.fromAddress("127.0.0.1:9013"), 7, "student", row);

        Assertions.assertTrue(success);
        Assertions.assertEquals("127.0.0.1:9013", target.get());
        Assertions.assertNotNull(captured.get());
        Assertions.assertEquals("INSERT", captured.get().getQueryAst().getStatementType());
        Assertions.assertEquals("student", captured.get().getQueryAst().getTableName());
        Assertions.assertEquals(2, captured.get().getQueryAst().getProjectionColumns().size());
        Assertions.assertEquals(2, captured.get().getQueryAst().getInsertValues().size());
    }
}
