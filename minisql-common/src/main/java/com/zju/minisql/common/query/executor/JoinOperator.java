package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 简化版等值 Join 算子。
 * 当前用于课程作业中的基础 Join 语义验证。
 */
public class JoinOperator implements PhysicalOperator {

    private final List<Row> leftRows;
    private final List<Row> rightRows;
    private final String leftColumn;
    private final String rightColumn;
    private Iterator<Row> outputIterator;

    public JoinOperator(List<Row> leftRows, List<Row> rightRows, String leftColumn, String rightColumn) {
        this.leftRows = leftRows;
        this.rightRows = rightRows;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    @Override
    public void open() {
        List<Row> resultRows = new ArrayList<>();
        for (Row leftRow : leftRows) {
            Object leftValue = leftRow.get(leftColumn);
            for (Row rightRow : rightRows) {
                Object rightValue = rightRow.get(rightColumn);
                if (leftValue != null && leftValue.equals(rightValue)) {
                    Row joinedRow = new Row();
                    for (Map.Entry<String, Object> entry : leftRow.getValues().entrySet()) {
                        joinedRow.put(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Object> entry : rightRow.getValues().entrySet()) {
                        if (!joinedRow.getValues().containsKey(entry.getKey())) {
                            joinedRow.put(entry.getKey(), entry.getValue());
                        }
                    }
                    resultRows.add(joinedRow);
                }
            }
        }
        outputIterator = resultRows.iterator();
    }

    @Override
    public Row next() {
        if (outputIterator != null && outputIterator.hasNext()) {
            return outputIterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        outputIterator = null;
    }
}
