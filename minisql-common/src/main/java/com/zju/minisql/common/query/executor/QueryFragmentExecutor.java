package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.AggregateBucket;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Worker 本地子任务执行器。
 */
public class QueryFragmentExecutor {

    /**
     * 提供按表名读取本地数据的抽象，便于单元测试和 Worker 实现复用。
     */
    @FunctionalInterface
    public interface TableRowProvider {
        List<Row> getTableRows(String tableName);
    }

    public PartialQueryResult execute(String workerAddress, QueryAst queryAst, List<Row> tableRows) {
        return execute(workerAddress, queryAst, tableName -> tableRows);
    }

    public PartialQueryResult execute(String workerAddress, QueryAst queryAst, TableRowProvider tableRowProvider) {
        PhysicalOperator operator = buildOperatorTree(queryAst, tableRowProvider);

        if (queryAst.hasAggregation()) {
            AggregateOperator aggregateOperator = new AggregateOperator(
                    operator,
                    queryAst.getGroupByColumns(),
                    queryAst.getAggregateCalls()
            );
            aggregateOperator.open();
            List<AggregateBucket> aggregateBuckets = aggregateOperator.snapshotBuckets();
            aggregateOperator.close();
            return PartialQueryResult.forAggregates(workerAddress, aggregateBuckets);
        }

        if (!queryAst.isSelectAll()) {
            operator = new ProjectOperator(operator, queryAst.getProjectionColumns());
        }

        List<Row> rows = collectRows(operator);
        return PartialQueryResult.forRows(workerAddress, rows);
    }

    private PhysicalOperator buildOperatorTree(QueryAst queryAst, TableRowProvider tableRowProvider) {
        PhysicalOperator operator;
        if (queryAst.hasJoin()) {
            List<Row> leftRows = qualifyRows(queryAst.getTableName(), safeRows(tableRowProvider.getTableRows(queryAst.getTableName())));
            List<Row> rightRows = qualifyRows(queryAst.getJoinTableName(), safeRows(tableRowProvider.getTableRows(queryAst.getJoinTableName())));
            operator = new JoinOperator(leftRows, rightRows, queryAst.getJoinLeftColumn(), queryAst.getJoinRightColumn());
        } else {
            operator = new ScanOperator(safeRows(tableRowProvider.getTableRows(queryAst.getTableName())));
        }

        if (queryAst.getFilterCondition() != null) {
            operator = new FilterOperator(operator, queryAst.getFilterCondition());
        }
        return operator;
    }

    private List<Row> collectRows(PhysicalOperator operator) {
        List<Row> rows = new ArrayList<>();
        operator.open();
        Row row;
        while ((row = operator.next()) != null) {
            rows.add(row);
        }
        operator.close();
        return rows;
    }

    private List<Row> safeRows(List<Row> rows) {
        return rows == null ? Collections.emptyList() : rows;
    }

    private List<Row> qualifyRows(String tableName, List<Row> rows) {
        List<Row> qualifiedRows = new ArrayList<>();
        for (Row row : rows) {
            Row qualifiedRow = new Row();
            for (Map.Entry<String, Object> entry : row.getValues().entrySet()) {
                qualifiedRow.put(tableName + "." + entry.getKey(), entry.getValue());
            }
            qualifiedRows.add(qualifiedRow);
        }
        return qualifiedRows;
    }
}
