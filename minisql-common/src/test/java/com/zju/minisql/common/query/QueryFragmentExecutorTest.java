package com.zju.minisql.common.query;

import com.zju.minisql.common.query.executor.QueryFragmentExecutor;
import com.zju.minisql.common.query.model.AggregateCall;
import com.zju.minisql.common.query.model.AggregateFunction;
import com.zju.minisql.common.query.model.FilterCondition;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.common.query.model.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Worker 本地执行器测试。
 */
public class QueryFragmentExecutorTest {

    private final QueryFragmentExecutor executor = new QueryFragmentExecutor();

    @Test
    public void testFilterAndProject() {
        QueryAst queryAst = new QueryAst();
        queryAst.setTableName("student");
        queryAst.setSelectAll(false);
        queryAst.setProjectionColumns(List.of("name"));
        queryAst.setFilterCondition(new FilterCondition("score", FilterCondition.ComparisonOperator.GTE, 90L));

        PartialQueryResult result = executor.execute("worker-1", queryAst, sampleStudentRows());
        assertEquals(2, result.getRows().size());
        assertEquals("Alice", result.getRows().get(0).get("name"));
        assertEquals(null, result.getRows().get(0).get("score"));
    }

    @Test
    public void testLocalAggregate() {
        QueryAst queryAst = new QueryAst();
        queryAst.setTableName("student");
        queryAst.setProjectionColumns(List.of("dept"));
        queryAst.setGroupByColumns(List.of("dept"));
        queryAst.setAggregateCalls(List.of(new AggregateCall(AggregateFunction.COUNT, "*", "cnt")));
        queryAst.setFilterCondition(new FilterCondition("score", FilterCondition.ComparisonOperator.GTE, 90L));

        PartialQueryResult result = executor.execute("worker-1", queryAst, sampleStudentRows());
        assertTrue(result.isAggregated());
        assertEquals(1, result.getAggregateBuckets().size());
        assertEquals("CS", result.getAggregateBuckets().get(0).getGroupValues().get("dept"));
        assertEquals(2L, result.getAggregateBuckets().get(0).getStates().get("cnt").finalValue());
    }

    @Test
    public void testLocalJoin() {
        QueryAst queryAst = new QueryAst();
        queryAst.setTableName("student");
        queryAst.setJoinTableName("score");
        queryAst.setJoinLeftColumn("student.id");
        queryAst.setJoinRightColumn("score.id");
        queryAst.setSelectAll(false);
        queryAst.setProjectionColumns(List.of("student.name", "score.course"));

        Map<String, List<Row>> tables = Map.of(
                "student", sampleStudentRows(),
                "score", sampleScoreRows()
        );
        PartialQueryResult result = executor.execute("worker-1", queryAst, tables::get);
        assertEquals(3, result.getRows().size());
        assertEquals("Alice", result.getRows().get(0).get("student.name"));
        assertEquals("DistributedDB", result.getRows().get(0).get("score.course"));
    }

    @Test
    public void testDropTableCommandDispatch() {
        QueryAst queryAst = new QueryAst();
        queryAst.setStatementType("DROP_TABLE");
        queryAst.setTableName("student");
        AtomicReference<String> droppedTable = new AtomicReference<>();

        PartialQueryResult result = executor.execute(
                "worker-1",
                queryAst,
                tableName -> List.of(),
                (tableName, row) -> {
                },
                droppedTable::set
        );

        assertTrue(result.getRows().isEmpty());
        assertEquals("student", droppedTable.get());
    }

    private List<Row> sampleStudentRows() {
        return List.of(
                Row.of("id", 1001L, "name", "Alice", "dept", "CS", "score", 95L),
                Row.of("id", 1002L, "name", "Bob", "dept", "EE", "score", 88L),
                Row.of("id", 1003L, "name", "Carol", "dept", "CS", "score", 91L)
        );
    }

    private List<Row> sampleScoreRows() {
        return List.of(
                Row.of("id", 1001L, "course", "DistributedDB", "grade", 95L),
                Row.of("id", 1002L, "course", "ComputerNetwork", "grade", 88L),
                Row.of("id", 1003L, "course", "OperatingSystem", "grade", 91L)
        );
    }
}
