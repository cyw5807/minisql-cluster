package com.zju.minisql.common.query.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 面向客户端的最终查询结果。
 */
public class QueryResult implements Serializable {

    private List<String> columns = new ArrayList<>();
    private List<Row> rows = new ArrayList<>();

    public QueryResult() {
    }

    public QueryResult(List<String> columns, List<Row> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public String toPrettyString() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.join(" | ", columns)).append(System.lineSeparator());
        for (Row row : rows) {
            StringJoiner joiner = new StringJoiner(" | ");
            for (String column : columns) {
                joiner.add(String.valueOf(row.get(column)));
            }
            builder.append(joiner).append(System.lineSeparator());
        }
        return builder.toString().trim();
    }
}
