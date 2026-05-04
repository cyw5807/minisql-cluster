package com.zju.minisql.common.query.model;

import java.io.Serializable;

/**
 * 单列过滤条件。
 * 当前阶段优先支持单表查询中的基础比较谓词。
 */
public class FilterCondition implements Serializable {

    public enum ComparisonOperator {
        EQ,
        NE,
        GT,
        GTE,
        LT,
        LTE
    }

    private String columnName;
    private ComparisonOperator operator;
    private Object value;

    public FilterCondition() {
    }

    public FilterCondition(String columnName, ComparisonOperator operator, Object value) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ComparisonOperator getOperator() {
        return operator;
    }

    public void setOperator(ComparisonOperator operator) {
        this.operator = operator;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
