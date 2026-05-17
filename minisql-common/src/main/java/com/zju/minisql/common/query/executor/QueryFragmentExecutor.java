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
     * 提供按表名读取本地数据的抽象
     */
    @FunctionalInterface
    public interface TableRowProvider {
        List<Row> getTableRows(String tableName);
    }

    /**
     * 🌟 新增：提供按表名写入本地数据的抽象
     */
    @FunctionalInterface
    public interface TableRowInserter {
        void insertRow(String tableName, Row row);
    }

    // 🌟 新增：包含写入能力的终极执行入口
    public PartialQueryResult execute(String workerAddress, QueryAst queryAst, TableRowProvider provider, TableRowInserter inserter) {
        // 拦截 INSERT 动作，执行纯粹的单行落盘
        if ("INSERT".equals(queryAst.getStatementType())) {
            Row newRow = new Row();
            List<String> columns = queryAst.getProjectionColumns(); // Master 传过来的 Schema
            List<Object> values = queryAst.getInsertValues();       // Parser 解析出的真实数据

            // 将单纯的值数组 [1001, "Alice"] 映射为键值对 {"id": 1001, "name": "Alice"}
            for (int i = 0; i < values.size(); i++) {
                String colName = (columns != null && columns.size() > i) ? columns.get(i) : ("col" + i);
                newRow.put(colName, values.get(i));
            }
            
            // 触发真实的磁盘/内存写入！
            inserter.insertRow(queryAst.getTableName(), newRow);
            
            // 插入操作不需要返回表格数据，返回一个空的集合作为 ACK 回执
            return PartialQueryResult.forRows(workerAddress, Collections.emptyList());
        }

        // 否则，转交原有的 SELECT 查询算子树逻辑
        return execute(workerAddress, queryAst, provider);
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