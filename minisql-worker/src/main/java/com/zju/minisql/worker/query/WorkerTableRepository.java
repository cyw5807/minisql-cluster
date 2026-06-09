package com.zju.minisql.worker.query;

import com.zju.minisql.common.query.model.Row;

import java.util.List;

/**
 * Worker 查询层的表数据读写抽象。
 */
public interface WorkerTableRepository {

    List<Row> getTableRows(String tableName);

    void insertRow(String tableName, Row row);

    void deleteTable(String tableName);
}
