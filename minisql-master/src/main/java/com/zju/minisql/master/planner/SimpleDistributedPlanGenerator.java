package com.zju.minisql.master.planner;

import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.master.router.ExecutionTarget;
import com.zju.minisql.master.router.QueryRouter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 简化版分布式计划生成器。
 */
public class SimpleDistributedPlanGenerator implements DistributedPlanGenerator {

    private final QueryRouter queryRouter;

    public SimpleDistributedPlanGenerator(QueryRouter queryRouter) {
        this.queryRouter = queryRouter;
    }

    @Override
    public DistributedExecutionPlan generate(LogicalPlan logicalPlan) {
        List<ExecutionTarget> executionTargets = queryRouter.route(logicalPlan);
        List<TaskFragment> fragments = new ArrayList<>();
        for (ExecutionTarget executionTarget : executionTargets) {
            fragments.add(new TaskFragment(
                    UUID.randomUUID().toString(),
                    executionTarget.getWorkerAddress(),
                    logicalPlan.getQueryAst()
            ));
        }

        DistributedExecutionPlan plan = new DistributedExecutionPlan();
        plan.setFragments(fragments);
        plan.setNeedCoordinatorMerge(logicalPlan.getQueryAst().hasAggregation() || fragments.size() > 1);
        return plan;
    }
}
