package com.zju.minisql.client.merger;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.query.model.AggregateBucket;
import com.zju.minisql.common.query.model.AggregateCall;
import com.zju.minisql.common.query.model.AggregateState;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.client.planner.LogicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * 协调节点结果合并器。
 */
public class ResultMerger {

    public QueryResult merge(LogicalPlan logicalPlan, List<PartialQueryResult> partialResults) {
        if (logicalPlan.getQueryAst().hasAggregation()) {
            return mergeAggregate(logicalPlan, partialResults);
        }
        return mergeRows(logicalPlan, partialResults);
    }

    private QueryResult mergeRows(LogicalPlan logicalPlan, List<PartialQueryResult> partialResults) {
        List<Row> mergedRows = new ArrayList<>();
        for (PartialQueryResult partialResult : partialResults) {
            mergedRows.addAll(partialResult.getRows());
        }
        return new QueryResult(resolveOutputColumns(logicalPlan), mergedRows);
    }

    private QueryResult mergeAggregate(LogicalPlan logicalPlan, List<PartialQueryResult> partialResults) {
        QueryAst queryAst = logicalPlan.getQueryAst();
        LinkedHashMap<String, AggregateBucket> mergedBuckets = new LinkedHashMap<>();

        for (PartialQueryResult partialResult : partialResults) {
            for (AggregateBucket aggregateBucket : partialResult.getAggregateBuckets()) {
                String bucketKey = buildBucketKey(queryAst.getGroupByColumns(), aggregateBucket);
                AggregateBucket targetBucket = mergedBuckets.computeIfAbsent(bucketKey, key -> createEmptyBucket(queryAst, aggregateBucket));
                for (AggregateCall aggregateCall : queryAst.getAggregateCalls()) {
                    AggregateState targetState = targetBucket.getStates().get(aggregateCall.getOutputName());
                    AggregateState sourceState = aggregateBucket.getStates().get(aggregateCall.getOutputName());
                    targetState.merge(sourceState);
                }
            }
        }

        List<Row> rows = new ArrayList<>();
        for (AggregateBucket aggregateBucket : mergedBuckets.values()) {
            Row row = new Row();
            for (String projectionColumn : queryAst.getProjectionColumns()) {
                row.put(projectionColumn, aggregateBucket.getGroupValues().get(projectionColumn));
            }
            for (AggregateCall aggregateCall : queryAst.getAggregateCalls()) {
                row.put(aggregateCall.getOutputName(), aggregateBucket.getStates().get(aggregateCall.getOutputName()).finalValue());
            }
            rows.add(row);
        }
        return new QueryResult(resolveOutputColumns(logicalPlan), rows);
    }

    private AggregateBucket createEmptyBucket(QueryAst queryAst, AggregateBucket seedBucket) {
        AggregateBucket bucket = new AggregateBucket();
        bucket.setGroupValues(new LinkedHashMap<>(seedBucket.getGroupValues()));
        LinkedHashMap<String, AggregateState> states = new LinkedHashMap<>();
        for (AggregateCall aggregateCall : queryAst.getAggregateCalls()) {
            states.put(aggregateCall.getOutputName(), new AggregateState(aggregateCall.getFunction()));
        }
        bucket.setStates(states);
        return bucket;
    }

    private String buildBucketKey(List<String> groupByColumns, AggregateBucket aggregateBucket) {
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            return "__all__";
        }
        StringJoiner joiner = new StringJoiner("|");
        for (String groupByColumn : groupByColumns) {
            joiner.add(String.valueOf(aggregateBucket.getGroupValues().get(groupByColumn)));
        }
        return joiner.toString();
    }

    private List<String> resolveOutputColumns(LogicalPlan logicalPlan) {
        QueryAst queryAst = logicalPlan.getQueryAst();
        List<String> columns = new ArrayList<>();
        if (queryAst.hasAggregation()) {
            columns.addAll(queryAst.getProjectionColumns());
            for (AggregateCall aggregateCall : queryAst.getAggregateCalls()) {
                columns.add(aggregateCall.getOutputName());
            }
            return columns;
        }
        if (queryAst.isSelectAll()) {
            for (ColumnMeta columnMeta : logicalPlan.getTableMeta().getColumns()) {
                columns.add(queryAst.hasJoin() ? logicalPlan.getTableMeta().getTableName() + "." + columnMeta.getColumnName() : columnMeta.getColumnName());
            }
            if (queryAst.hasJoin() && logicalPlan.getJoinTableMeta() != null) {
                for (ColumnMeta columnMeta : logicalPlan.getJoinTableMeta().getColumns()) {
                    columns.add(logicalPlan.getJoinTableMeta().getTableName() + "." + columnMeta.getColumnName());
                }
            }
            return columns;
        }
        columns.addAll(queryAst.getProjectionColumns());
        return columns;
    }
}
