package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.Row;

import java.util.List;

/**
 * 列裁剪算子。
 */
public class ProjectOperator implements PhysicalOperator {

    private final PhysicalOperator child;
    private final List<String> columns;

    public ProjectOperator(PhysicalOperator child, List<String> columns) {
        this.child = child;
        this.columns = columns;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Row next() {
        Row row = child.next();
        if (row == null) {
            return null;
        }
        return row.project(columns);
    }

    @Override
    public void close() {
        child.close();
    }
}
