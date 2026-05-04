package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.Row;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 本地扫描算子。
 */
public class ScanOperator implements PhysicalOperator {

    private final List<Row> rows;
    private Iterator<Row> iterator;

    public ScanOperator(List<Row> rows) {
        this.rows = rows == null ? Collections.emptyList() : rows;
    }

    @Override
    public void open() {
        iterator = rows.iterator();
    }

    @Override
    public Row next() {
        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        iterator = null;
    }
}
