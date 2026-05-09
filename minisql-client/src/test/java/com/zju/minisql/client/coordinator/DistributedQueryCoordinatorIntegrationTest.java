package com.zju.minisql.client.coordinator;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.query.executor.QueryFragmentExecutor;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.common.query.service.DistributedQueryTaskService;
import com.zju.minisql.common.rpc.server.NettyRpcServer;
import com.zju.minisql.common.rpc.server.ServiceProvider;
import com.zju.minisql.client.network.RpcFragmentTaskClient;
import com.zju.minisql.client.merger.ResultMerger;
import com.zju.minisql.client.metadata.TableMetadataProvider;
import com.zju.minisql.client.parser.JSqlParserSqlParser;
import com.zju.minisql.client.planner.SimpleDistributedPlanGenerator;
import com.zju.minisql.client.planner.SimpleLogicalPlanner;
import com.zju.minisql.client.router.HashQueryRouter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 分布式查询全链路集成测试。
 */
public class DistributedQueryCoordinatorIntegrationTest {

    @Test
    public void testCoordinatorExecute() throws Exception {
        int port1 = findFreePort();
        int port2 = findFreePort();
        List<String> workerAddresses = new ArrayList<>(List.of("127.0.0.1:" + port1, "127.0.0.1:" + port2));
        workerAddresses.sort(Comparator.naturalOrder());

        Map<String, Map<String, List<Row>>> partitionedTables = partitionTables(workerAddresses);
        startWorker(workerAddresses.get(0), partitionedTables.get(workerAddresses.get(0)));
        startWorker(workerAddresses.get(1), partitionedTables.get(workerAddresses.get(1)));
        Thread.sleep(1000);

        TableMetadataProvider metadataProvider = tableName -> tableMeta(tableName);
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(
                new JSqlParserSqlParser(),
                new SimpleLogicalPlanner(metadataProvider),
                new SimpleDistributedPlanGenerator(new HashQueryRouter(() -> workerAddresses)),
                new RpcFragmentTaskClient(),
                new ResultMerger()
        );

        QueryResult pointQueryResult = coordinator.execute("SELECT name FROM student WHERE id = 1001");
        assertEquals(1, pointQueryResult.getRows().size());
        assertEquals("Alice", pointQueryResult.getRows().get(0).get("name"));

        QueryResult aggregateResult = coordinator.execute("SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept");
        assertEquals(2, aggregateResult.getRows().size());
        Map<String, Long> countByDept = new HashMap<>();
        for (Row row : aggregateResult.getRows()) {
            countByDept.put(String.valueOf(row.get("dept")), ((Number) row.get("cnt")).longValue());
        }
        assertEquals(2L, countByDept.get("CS"));
        assertEquals(1L, countByDept.get("EE"));

        QueryResult joinResult = coordinator.execute("SELECT student.name, score.course FROM student JOIN score ON student.id = score.id");
        assertEquals(5, joinResult.getRows().size());
        assertEquals("Alice", joinResult.getRows().get(0).get("student.name"));
        assertEquals("DistributedDB", joinResult.getRows().get(0).get("score.course"));
    }

    private void startWorker(String workerAddress, Map<String, List<Row>> tableRows) {
        ServiceProvider serviceProvider = new ServiceProvider();
        serviceProvider.registerService(new TestDistributedQueryTaskService(workerAddress, tableRows));
        Thread serverThread = new Thread(() -> {
            NettyRpcServer server = new NettyRpcServer(serviceProvider);
            server.start(Integer.parseInt(workerAddress.split(":")[1]));
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

    private Map<String, Map<String, List<Row>>> partitionTables(List<String> workerAddresses) {
        Map<String, Map<String, List<Row>>> partitions = new HashMap<>();
        for (String workerAddress : workerAddresses) {
            Map<String, List<Row>> tables = new HashMap<>();
            tables.put("student", new ArrayList<>());
            tables.put("score", new ArrayList<>());
            partitions.put(workerAddress, tables);
        }

        List<Row> students = List.of(
                Row.of("id", 1001L, "name", "Alice", "dept", "CS", "score", 95L),
                Row.of("id", 1002L, "name", "Bob", "dept", "EE", "score", 88L),
                Row.of("id", 1003L, "name", "Carol", "dept", "CS", "score", 91L),
                Row.of("id", 1004L, "name", "David", "dept", "ME", "score", 84L),
                Row.of("id", 1005L, "name", "Eve", "dept", "EE", "score", 97L)
        );
        List<Row> scores = List.of(
                Row.of("id", 1001L, "course", "DistributedDB", "grade", 95L),
                Row.of("id", 1002L, "course", "ComputerNetwork", "grade", 88L),
                Row.of("id", 1003L, "course", "OperatingSystem", "grade", 91L),
                Row.of("id", 1004L, "course", "MechanicalDesign", "grade", 84L),
                Row.of("id", 1005L, "course", "PowerSystem", "grade", 97L)
        );

        partitionRows("student", students, workerAddresses, partitions);
        partitionRows("score", scores, workerAddresses, partitions);
        return partitions;
    }

    private void partitionRows(String tableName,
                               List<Row> rows,
                               List<String> workerAddresses,
                               Map<String, Map<String, List<Row>>> partitions) {
        for (Row row : rows) {
            int partitionId = Math.floorMod(String.valueOf(row.get("id")).hashCode(), workerAddresses.size());
            partitions.get(workerAddresses.get(partitionId)).get(tableName).add(row);
        }
    }

    private TableMeta tableMeta(String tableName) {
        if ("student".equalsIgnoreCase(tableName)) {
            TableMeta tableMeta = new TableMeta("student");
            tableMeta.addColumn(new ColumnMeta("id", "INT", 0, true, true));
            tableMeta.addColumn(new ColumnMeta("name", "CHAR", 20, false, false));
            tableMeta.addColumn(new ColumnMeta("dept", "CHAR", 20, false, false));
            tableMeta.addColumn(new ColumnMeta("score", "INT", 0, false, false));
            return tableMeta;
        }
        if ("score".equalsIgnoreCase(tableName)) {
            TableMeta tableMeta = new TableMeta("score");
            tableMeta.addColumn(new ColumnMeta("id", "INT", 0, true, true));
            tableMeta.addColumn(new ColumnMeta("course", "CHAR", 32, false, false));
            tableMeta.addColumn(new ColumnMeta("grade", "INT", 0, false, false));
            return tableMeta;
        }
        throw new IllegalArgumentException("未知测试表: " + tableName);
    }

    private int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    public static class TestDistributedQueryTaskService implements DistributedQueryTaskService {
        private final String workerAddress;
        private final Map<String, List<Row>> tableRows;
        private final QueryFragmentExecutor executor = new QueryFragmentExecutor();

        private TestDistributedQueryTaskService(String workerAddress, Map<String, List<Row>> tableRows) {
            this.workerAddress = workerAddress;
            this.tableRows = tableRows;
        }

        @Override
        public PartialQueryResult executeFragment(TaskFragment fragment) {
            return executor.execute(workerAddress, fragment.getQueryAst(), tableRows::get);
        }
    }
}
