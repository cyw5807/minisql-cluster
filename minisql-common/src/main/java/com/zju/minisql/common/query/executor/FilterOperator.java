package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.FilterCondition;
import com.zju.minisql.common.query.model.Row;

/**
 * WHERE 过滤算子。
 */
public class FilterOperator implements PhysicalOperator {

    private final PhysicalOperator child;
    private final FilterCondition condition;

    public FilterOperator(PhysicalOperator child, FilterCondition condition) {
        this.child = child;
        this.condition = condition;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Row next() {
        Row row;
        while ((row = child.next()) != null) {
            if (matches(row, condition)) {
                return row;
            }
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean matches(Row row, FilterCondition condition) {
        if (condition == null) {
            return true;
        }
        Object left = row.get(condition.getColumnName());
        Object right = condition.getValue();
        int compare;
        if (left instanceof Number && right instanceof Number) {
            compare = Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
        } else if (left == null && right == null) {
            compare = 0;
        } else if (left == null) {
            compare = -1;
        } else if (right == null) {
            compare = 1;
        } else {
            compare = ((Comparable) left).compareTo(right);
        }

        switch (condition.getOperator()) {
            case EQ:
                return compare == 0;
            case NE:
                return compare != 0;
            case GT:
                return compare > 0;
            case GTE:
                return compare >= 0;
            case LT:
                return compare < 0;
            case LTE:
                return compare <= 0;
            default:
                throw new IllegalStateException("未知比较操作符: " + condition.getOperator());
        }
    }
}
