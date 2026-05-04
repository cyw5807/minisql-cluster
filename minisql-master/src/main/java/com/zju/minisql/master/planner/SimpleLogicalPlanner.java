package com.zju.minisql.master.planner;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.query.model.AggregateCall;
import com.zju.minisql.common.query.model.FilterCondition;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.master.metadata.TableMetadataProvider;

import java.util.HashSet;
import java.util.Set;

/**
 * 简化版逻辑计划生成器。
 * 当前重点放在元数据校验和语义约束检查上。
 */
public class SimpleLogicalPlanner implements LogicalPlanner {

    private final TableMetadataProvider tableMetadataProvider;

    public SimpleLogicalPlanner(TableMetadataProvider tableMetadataProvider) {
        this.tableMetadataProvider = tableMetadataProvider;
    }

    @Override
    public LogicalPlan build(QueryAst queryAst) {
        try {
            TableMeta tableMeta = tableMetadataProvider.getTable(queryAst.getTableName());
            if (tableMeta == null) {
                throw new IllegalArgumentException("表不存在: " + queryAst.getTableName());
            }

            TableMeta joinTableMeta = null;
            if (queryAst.hasJoin()) {
                joinTableMeta = tableMetadataProvider.getTable(queryAst.getJoinTableName());
                if (joinTableMeta == null) {
                    throw new IllegalArgumentException("Join 表不存在: " + queryAst.getJoinTableName());
                }
            }

            validateColumns(queryAst, tableMeta, joinTableMeta);
            return new LogicalPlan(queryAst, tableMeta, joinTableMeta);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("构建逻辑计划失败", e);
        }
    }

    private void validateColumns(QueryAst queryAst, TableMeta tableMeta, TableMeta joinTableMeta) {
        Set<String> availableColumns = buildAvailableColumns(queryAst.getTableName(), tableMeta);
        if (joinTableMeta != null) {
            availableColumns.addAll(buildAvailableColumns(queryAst.getJoinTableName(), joinTableMeta));
        }

        if (queryAst.getFilterCondition() != null) {
            assertColumnExists(queryAst.getFilterCondition(), availableColumns);
        }

        for (String projectionColumn : queryAst.getProjectionColumns()) {
            assertColumnExists(projectionColumn, availableColumns);
        }

        for (String groupByColumn : queryAst.getGroupByColumns()) {
            assertColumnExists(groupByColumn, availableColumns);
        }

        for (AggregateCall aggregateCall : queryAst.getAggregateCalls()) {
            if (!aggregateCall.isCountAll()) {
                assertColumnExists(aggregateCall.getColumnName(), availableColumns);
            }
        }

        if (queryAst.hasJoin()) {
            assertColumnExists(queryAst.getJoinLeftColumn(), availableColumns);
            assertColumnExists(queryAst.getJoinRightColumn(), availableColumns);
        }

        if (queryAst.hasAggregation()) {
            Set<String> groupColumns = new HashSet<>();
            for (String groupByColumn : queryAst.getGroupByColumns()) {
                groupColumns.add(groupByColumn.toLowerCase());
            }
            for (String projectionColumn : queryAst.getProjectionColumns()) {
                if (!groupColumns.contains(projectionColumn.toLowerCase())) {
                    throw new IllegalArgumentException("聚合查询中，非聚合列必须出现在 GROUP BY 中: " + projectionColumn);
                }
            }
        }
    }

    private Set<String> buildAvailableColumns(String tableName, TableMeta tableMeta) {
        Set<String> availableColumns = new HashSet<>();
        for (ColumnMeta columnMeta : tableMeta.getColumns()) {
            availableColumns.add(columnMeta.getColumnName().toLowerCase());
            availableColumns.add((tableName + "." + columnMeta.getColumnName()).toLowerCase());
        }
        return availableColumns;
    }

    private void assertColumnExists(FilterCondition filterCondition, Set<String> availableColumns) {
        assertColumnExists(filterCondition.getColumnName(), availableColumns);
    }

    private void assertColumnExists(String columnName, Set<String> availableColumns) {
        if (!availableColumns.contains(columnName.toLowerCase())) {
            throw new IllegalArgumentException("字段不存在: " + columnName);
        }
    }
}
