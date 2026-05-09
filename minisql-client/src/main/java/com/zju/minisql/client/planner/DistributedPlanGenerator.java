package com.zju.minisql.client.planner;

/**
 * 分布式计划生成接口。
 */
public interface DistributedPlanGenerator {

    DistributedExecutionPlan generate(LogicalPlan logicalPlan);
}
