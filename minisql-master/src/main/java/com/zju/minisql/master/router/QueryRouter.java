package com.zju.minisql.master.router;

import com.zju.minisql.master.planner.LogicalPlan;

import java.util.List;

/**
 * 查询路由接口。
 */
public interface QueryRouter {

    List<ExecutionTarget> route(LogicalPlan logicalPlan);
}
