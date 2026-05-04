package com.zju.minisql.common.query.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 行数据的通用表示。
 * 为了方便调试与序列化，这里直接使用有序 Map 保存列值。
 */
public class Row implements Serializable {

    private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();

    public Row put(String columnName, Object value) {
        values.put(columnName, value);
        return this;
    }

    public Object get(String columnName) {
        if (values.containsKey(columnName)) {
            return values.get(columnName);
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(columnName)) {
                return entry.getValue();
            }
        }

        // 如果直接找不到，则尝试按“表名.列名”的后缀匹配，便于 Join 后读取限定列。
        Object matchedValue = null;
        String qualifiedSuffix = "." + columnName.toLowerCase();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getKey().toLowerCase().endsWith(qualifiedSuffix)) {
                matchedValue = entry.getValue();
                break;
            }
        }
        return matchedValue;
    }

    public LinkedHashMap<String, Object> getValues() {
        return values;
    }

    public Row project(List<String> columns) {
        Row projectedRow = new Row();
        for (String column : columns) {
            projectedRow.put(column, get(column));
        }
        return projectedRow;
    }

    public static Row of(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Row.of 需要传入偶数个参数");
        }
        Row row = new Row();
        for (int i = 0; i < keyValues.length; i += 2) {
            row.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return row;
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
