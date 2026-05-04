package com.zju.minisql.master.parser;

import com.zju.minisql.common.query.model.QueryAst;

/**
 * SQL 解析接口。
 */
public interface SqlParser {

    QueryAst parse(String sql);
}
