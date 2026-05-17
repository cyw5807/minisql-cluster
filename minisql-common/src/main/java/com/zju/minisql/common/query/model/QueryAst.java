package com.zju.minisql.common.query.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL 解析后的抽象语法树简化表示。
 * 【纯净版】：不依赖任何第三方解析库，完全解耦。
 */
public class QueryAst implements Serializable {

    // ==========================================
    // 自定义内部类：用于解耦 JSqlParser 的列定义
    // ==========================================
    public static class ColumnDef implements Serializable {
        private String name;
        private String dataType;

        public ColumnDef(String name, String dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        public String getName() { return name; }
        public String getDataType() { return dataType; }
    }

    // ==========================================
    // 新增：支持完整 DDL/DML 语法的扩展字段
    // ==========================================
    private String statementType; 
    private List<Object> insertValues; 
    private List<ColumnDef> columnDefinitions; // 改用我们自定义的纯净对象

    // ==========================================
    // 原有：SELECT 查询相关的基础字段
    // ==========================================
    private String tableName;
    private String joinTableName;
    private String joinLeftColumn;
    private String joinRightColumn;
    private boolean selectAll;
    private List<String> projectionColumns = new ArrayList<>();
    private FilterCondition filterCondition;
    private List<String> groupByColumns = new ArrayList<>();
    private List<AggregateCall> aggregateCalls = new ArrayList<>();

    // ==========================================
    // 新增字段的 Getter 和 Setter 方法
    // ==========================================

    public String getStatementType() {
        return statementType;
    }

    public void setStatementType(String statementType) {
        this.statementType = statementType;
    }

    public List<Object> getInsertValues() {
        return insertValues;
    }

    public void setInsertValues(List<Object> insertValues) {
        this.insertValues = insertValues;
    }

    public List<ColumnDef> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDef> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    // ==========================================
    // 原有字段的 Getter 和 Setter 方法
    // ==========================================

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getJoinTableName() { return joinTableName; }
    public void setJoinTableName(String joinTableName) { this.joinTableName = joinTableName; }

    public String getJoinLeftColumn() { return joinLeftColumn; }
    public void setJoinLeftColumn(String joinLeftColumn) { this.joinLeftColumn = joinLeftColumn; }

    public String getJoinRightColumn() { return joinRightColumn; }
    public void setJoinRightColumn(String joinRightColumn) { this.joinRightColumn = joinRightColumn; }

    public boolean isSelectAll() { return selectAll; }
    public void setSelectAll(boolean selectAll) { this.selectAll = selectAll; }

    public List<String> getProjectionColumns() { return projectionColumns; }
    public void setProjectionColumns(List<String> projectionColumns) { this.projectionColumns = projectionColumns; }

    public FilterCondition getFilterCondition() { return filterCondition; }
    public void setFilterCondition(FilterCondition filterCondition) { this.filterCondition = filterCondition; }

    public List<String> getGroupByColumns() { return groupByColumns; }
    public void setGroupByColumns(List<String> groupByColumns) { this.groupByColumns = groupByColumns; }

    public List<AggregateCall> getAggregateCalls() { return aggregateCalls; }
    public void setAggregateCalls(List<AggregateCall> aggregateCalls) { this.aggregateCalls = aggregateCalls; }

    public boolean hasAggregation() { return aggregateCalls != null && !aggregateCalls.isEmpty(); }
    public boolean hasJoin() { return joinTableName != null && !joinTableName.isBlank(); }
}