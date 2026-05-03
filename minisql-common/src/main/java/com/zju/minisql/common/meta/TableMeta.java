package com.zju.minisql.common.meta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 表结构的元数据定义
 */
public class TableMeta implements Serializable {

    private String tableName;
    private List<ColumnMeta> columns;

    public TableMeta() {
        this.columns = new ArrayList<>();
    }

    public TableMeta(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }

    public void addColumn(ColumnMeta column) {
        this.columns.add(column);
    }

    /**
     * 获取表的主键列
     * 如果没有主键则返回 null
     */
    public ColumnMeta getPrimaryKey() {
        for (ColumnMeta col : columns) {
            if (col.isPrimaryKey()) {
                return col;
            }
        }
        return null;
    }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public List<ColumnMeta> getColumns() { return columns; }
    public void setColumns(List<ColumnMeta> columns) { this.columns = columns; }
}