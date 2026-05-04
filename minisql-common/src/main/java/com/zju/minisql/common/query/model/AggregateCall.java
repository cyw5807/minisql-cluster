package com.zju.minisql.common.query.model;

import java.io.Serializable;

/**
 * 聚合函数调用描述。
 */
public class AggregateCall implements Serializable {

    private AggregateFunction function;
    private String columnName;
    private String alias;

    public AggregateCall() {
    }

    public AggregateCall(AggregateFunction function, String columnName, String alias) {
        this.function = function;
        this.columnName = columnName;
        this.alias = alias;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public void setFunction(AggregateFunction function) {
        this.function = function;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public boolean isCountAll() {
        return function == AggregateFunction.COUNT && (columnName == null || "*".equals(columnName));
    }

    public String getOutputName() {
        if (alias != null && !alias.isBlank()) {
            return alias;
        }
        if (isCountAll()) {
            return "count_all";
        }
        return function.name().toLowerCase() + "_" + columnName;
    }
}
