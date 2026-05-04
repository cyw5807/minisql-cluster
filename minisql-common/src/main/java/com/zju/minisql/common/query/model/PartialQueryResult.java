package com.zju.minisql.common.query.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Worker 返回给 Coordinator 的局部结果。
 */
public class PartialQueryResult implements Serializable {

    private boolean aggregated;
    private String workerAddress;
    private List<Row> rows = new ArrayList<>();
    private List<AggregateBucket> aggregateBuckets = new ArrayList<>();

    public static PartialQueryResult forRows(String workerAddress, List<Row> rows) {
        PartialQueryResult result = new PartialQueryResult();
        result.setWorkerAddress(workerAddress);
        result.setAggregated(false);
        result.setRows(rows);
        return result;
    }

    public static PartialQueryResult forAggregates(String workerAddress, List<AggregateBucket> aggregateBuckets) {
        PartialQueryResult result = new PartialQueryResult();
        result.setWorkerAddress(workerAddress);
        result.setAggregated(true);
        result.setAggregateBuckets(aggregateBuckets);
        return result;
    }

    public boolean isAggregated() {
        return aggregated;
    }

    public void setAggregated(boolean aggregated) {
        this.aggregated = aggregated;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public void setWorkerAddress(String workerAddress) {
        this.workerAddress = workerAddress;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public List<AggregateBucket> getAggregateBuckets() {
        return aggregateBuckets;
    }

    public void setAggregateBuckets(List<AggregateBucket> aggregateBuckets) {
        this.aggregateBuckets = aggregateBuckets;
    }
}
