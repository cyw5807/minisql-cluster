package com.zju.minisql.master.planner;

import com.zju.minisql.common.query.model.QueryAst;

/**
 * 逻辑计划生成接口。
 */
public interface LogicalPlanner {

    LogicalPlan build(QueryAst queryAst);
}
