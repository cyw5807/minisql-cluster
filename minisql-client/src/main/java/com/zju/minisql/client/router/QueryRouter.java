package com.zju.minisql.client.router;

import com.zju.minisql.client.planner.LogicalPlan;

import java.util.List;

/**
 * 查询路由接口。
 */
public interface QueryRouter {

    List<ExecutionTarget> route(LogicalPlan logicalPlan);
}
