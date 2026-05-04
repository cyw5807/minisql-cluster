package com.zju.minisql.common.query.model;

import java.io.Serializable;

/**
 * 聚合函数类型。
 */
public enum AggregateFunction implements Serializable {
    COUNT,
    SUM,
    AVG,
    MAX,
    MIN
}
