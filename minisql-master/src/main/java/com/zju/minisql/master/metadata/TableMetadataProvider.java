package com.zju.minisql.master.metadata;

import com.zju.minisql.common.meta.TableMeta;

/**
 * 表元数据查询抽象。
 * 这样可以让查询引擎既能对接真实 ZK 元数据，也能在测试里注入内存实现。
 */
public interface TableMetadataProvider {

    TableMeta getTable(String tableName) throws Exception;
}
