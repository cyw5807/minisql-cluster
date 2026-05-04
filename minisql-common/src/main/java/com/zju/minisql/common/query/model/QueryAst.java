package com.zju.minisql.common.query.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL 解析后的抽象语法树简化表示。
 */
public class QueryAst implements Serializable {

    private String tableName;
    private String joinTableName;
    private String joinLeftColumn;
    private String joinRightColumn;
    private boolean selectAll;
    private List<String> projectionColumns = new ArrayList<>();
    private FilterCondition filterCondition;
    private List<String> groupByColumns = new ArrayList<>();
    private List<AggregateCall> aggregateCalls = new ArrayList<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getJoinTableName() {
        return joinTableName;
    }

    public void setJoinTableName(String joinTableName) {
        this.joinTableName = joinTableName;
    }

    public String getJoinLeftColumn() {
        return joinLeftColumn;
    }

    public void setJoinLeftColumn(String joinLeftColumn) {
        this.joinLeftColumn = joinLeftColumn;
    }

    public String getJoinRightColumn() {
        return joinRightColumn;
    }

    public void setJoinRightColumn(String joinRightColumn) {
        this.joinRightColumn = joinRightColumn;
    }

    public boolean isSelectAll() {
        return selectAll;
    }

    public void setSelectAll(boolean selectAll) {
        this.selectAll = selectAll;
    }

    public List<String> getProjectionColumns() {
        return projectionColumns;
    }

    public void setProjectionColumns(List<String> projectionColumns) {
        this.projectionColumns = projectionColumns;
    }

    public FilterCondition getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(FilterCondition filterCondition) {
        this.filterCondition = filterCondition;
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public void setGroupByColumns(List<String> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    public List<AggregateCall> getAggregateCalls() {
        return aggregateCalls;
    }

    public void setAggregateCalls(List<AggregateCall> aggregateCalls) {
        this.aggregateCalls = aggregateCalls;
    }

    public boolean hasAggregation() {
        return aggregateCalls != null && !aggregateCalls.isEmpty();
    }

    public boolean hasJoin() {
        return joinTableName != null && !joinTableName.isBlank();
    }
}
