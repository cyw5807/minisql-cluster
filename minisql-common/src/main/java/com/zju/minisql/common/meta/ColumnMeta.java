package com.zju.minisql.common.meta;

import java.io.Serializable;

/**
 * 表字段/列的元数据定义
 */
public class ColumnMeta implements Serializable {

    private String columnName;
    // 数据类型，如 INT, FLOAT, CHAR
    private String dataType;
    // 针对 CHAR(n) 类型的长度限制，其他类型默认为 0
    private int length;
    private boolean isPrimaryKey;
    private boolean isUnique;

    public ColumnMeta() {
    }

    public ColumnMeta(String columnName, String dataType, int length, boolean isPrimaryKey, boolean isUnique) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.length = length;
        this.isPrimaryKey = isPrimaryKey;
        this.isUnique = isUnique;
    }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }
    public int getLength() { return length; }
    public void setLength(int length) { this.length = length; }
    public boolean isPrimaryKey() { return isPrimaryKey; }
    public void setPrimaryKey(boolean primaryKey) { isPrimaryKey = primaryKey; }
    public boolean isUnique() { return isUnique; }
    public void setUnique(boolean unique) { isUnique = unique; }
}