package com.zju.minisql.common.query.executor;

import com.zju.minisql.common.query.model.Row;

/**
 * 物理算子统一接口。
 */
public interface PhysicalOperator {

    void open();

    Row next();

    void close();
}
