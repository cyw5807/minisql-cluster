package com.zju.minisql.common.query.model;

import java.io.Serializable;

/**
 * 聚合中间状态。
 * 该对象既用于 Worker 的局部聚合，也用于 Coordinator 的最终合并。
 */
public class AggregateState implements Serializable {

    private AggregateFunction function;
    private double sum;
    private long count;
    private Object min;
    private Object max;

    public AggregateState() {
    }

    public AggregateState(AggregateFunction function) {
        this.function = function;
    }

    public void apply(Object value) {
        switch (function) {
            case COUNT:
                count++;
                break;
            case SUM:
                if (value != null) {
                    sum += toDouble(value);
                }
                break;
            case AVG:
                if (value != null) {
                    sum += toDouble(value);
                    count++;
                }
                break;
            case MAX:
                if (value != null && (max == null || compare(value, max) > 0)) {
                    max = value;
                }
                break;
            case MIN:
                if (value != null && (min == null || compare(value, min) < 0)) {
                    min = value;
                }
                break;
            default:
                throw new IllegalStateException("未知聚合函数: " + function);
        }
    }

    public void merge(AggregateState other) {
        if (other == null) {
            return;
        }
        switch (function) {
            case COUNT:
                this.count += other.count;
                break;
            case SUM:
                this.sum += other.sum;
                break;
            case AVG:
                this.sum += other.sum;
                this.count += other.count;
                break;
            case MAX:
                if (other.max != null && (this.max == null || compare(other.max, this.max) > 0)) {
                    this.max = other.max;
                }
                break;
            case MIN:
                if (other.min != null && (this.min == null || compare(other.min, this.min) < 0)) {
                    this.min = other.min;
                }
                break;
            default:
                throw new IllegalStateException("未知聚合函数: " + function);
        }
    }

    public Object finalValue() {
        switch (function) {
            case COUNT:
                return count;
            case SUM:
                return normalizeNumber(sum);
            case AVG:
                if (count == 0) {
                    return null;
                }
                return sum / count;
            case MAX:
                return max;
            case MIN:
                return min;
            default:
                throw new IllegalStateException("未知聚合函数: " + function);
        }
    }

    private static double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compare(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
        }
        return ((Comparable) left).compareTo(right);
    }

    private static Object normalizeNumber(double value) {
        if (Math.rint(value) == value) {
            return (long) value;
        }
        return value;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public void setFunction(AggregateFunction function) {
        this.function = function;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }
}
