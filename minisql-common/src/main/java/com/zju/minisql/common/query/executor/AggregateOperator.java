package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.AggregateBucket;
import com.zju.minisql.common.query.model.AggregateCall;
import com.zju.minisql.common.query.model.AggregateState;
import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * 聚合算子。
 * 该算子会一次性消费子节点全部输入，适合课程作业阶段的简化实现。
 */
public class AggregateOperator implements PhysicalOperator {

    private final PhysicalOperator child;
    private final List<String> groupByColumns;
    private final List<AggregateCall> aggregateCalls;
    private final LinkedHashMap<String, AggregateBucket> bucketMap = new LinkedHashMap<>();
    private Iterator<Row> outputIterator;

    public AggregateOperator(PhysicalOperator child, List<String> groupByColumns, List<AggregateCall> aggregateCalls) {
        this.child = child;
        this.groupByColumns = groupByColumns;
        this.aggregateCalls = aggregateCalls;
    }

    @Override
    public void open() {
        bucketMap.clear();
        child.open();

        Row row;
        while ((row = child.next()) != null) {
            String groupKey = buildGroupKey(row);
            AggregateBucket bucket = bucketMap.get(groupKey);
            if (bucket == null) {
                bucket = createBucket(row);
                bucketMap.put(groupKey, bucket);
            }
            for (AggregateCall aggregateCall : aggregateCalls) {
                AggregateState state = bucket.getStates().get(aggregateCall.getOutputName());
                Object value = aggregateCall.isCountAll() ? null : row.get(aggregateCall.getColumnName());
                state.apply(value);
            }
        }

        // SQL 语义要求：无 GROUP BY 的空输入上，聚合仍返回一行结果。
        if (bucketMap.isEmpty() && (groupByColumns == null || groupByColumns.isEmpty())) {
            AggregateBucket emptyBucket = createBucket(null);
            bucketMap.put("__empty__", emptyBucket);
        }

        List<Row> outputRows = new ArrayList<>();
        for (AggregateBucket bucket : bucketMap.values()) {
            Row outputRow = new Row();
            for (Map.Entry<String, Object> entry : bucket.getGroupValues().entrySet()) {
                outputRow.put(entry.getKey(), entry.getValue());
            }
            for (AggregateCall aggregateCall : aggregateCalls) {
                AggregateState state = bucket.getStates().get(aggregateCall.getOutputName());
                outputRow.put(aggregateCall.getOutputName(), state.finalValue());
            }
            outputRows.add(outputRow);
        }
        outputIterator = outputRows.iterator();
    }

    @Override
    public Row next() {
        if (outputIterator != null && outputIterator.hasNext()) {
            return outputIterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
        outputIterator = null;
    }

    public List<AggregateBucket> snapshotBuckets() {
        return new ArrayList<>(bucketMap.values());
    }

    private String buildGroupKey(Row row) {
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            return "__all__";
        }
        StringJoiner joiner = new StringJoiner("|");
        for (String column : groupByColumns) {
            joiner.add(String.valueOf(row.get(column)));
        }
        return joiner.toString();
    }

    private AggregateBucket createBucket(Row row) {
        AggregateBucket bucket = new AggregateBucket();
        LinkedHashMap<String, Object> groupValues = new LinkedHashMap<>();
        if (groupByColumns != null) {
            for (String groupByColumn : groupByColumns) {
                groupValues.put(groupByColumn, row == null ? null : row.get(groupByColumn));
            }
        }
        LinkedHashMap<String, AggregateState> states = new LinkedHashMap<>();
        for (AggregateCall aggregateCall : aggregateCalls) {
            states.put(aggregateCall.getOutputName(), new AggregateState(aggregateCall.getFunction()));
        }
        bucket.setGroupValues(groupValues);
        bucket.setStates(states);
        return bucket;
    }
}
