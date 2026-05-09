package com.zju.minisql.client.coordinator;

import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.client.network.FragmentTaskClient;
import com.zju.minisql.client.merger.ResultMerger;
import com.zju.minisql.client.parser.SqlParser;
import com.zju.minisql.client.planner.DistributedExecutionPlan;
import com.zju.minisql.client.planner.DistributedPlanGenerator;
import com.zju.minisql.client.planner.LogicalPlan;
import com.zju.minisql.client.planner.LogicalPlanner;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式查询协调器。
 * 负责串联“解析 -> 计划 -> 路由 -> 下发 -> 汇总”完整流程。
 */
public class DistributedQueryCoordinator {

    private final SqlParser sqlParser;
    private final LogicalPlanner logicalPlanner;
    private final DistributedPlanGenerator distributedPlanGenerator;
    private final FragmentTaskClient fragmentTaskClient;
    private final ResultMerger resultMerger;

    public DistributedQueryCoordinator(SqlParser sqlParser,
                                       LogicalPlanner logicalPlanner,
                                       DistributedPlanGenerator distributedPlanGenerator,
                                       FragmentTaskClient fragmentTaskClient,
                                       ResultMerger resultMerger) {
        this.sqlParser = sqlParser;
        this.logicalPlanner = logicalPlanner;
        this.distributedPlanGenerator = distributedPlanGenerator;
        this.fragmentTaskClient = fragmentTaskClient;
        this.resultMerger = resultMerger;
    }

    public QueryResult execute(String sql) {
        LogicalPlan logicalPlan = logicalPlanner.build(sqlParser.parse(sql));
        DistributedExecutionPlan distributedExecutionPlan = distributedPlanGenerator.generate(logicalPlan);

        List<PartialQueryResult> partialResults = new ArrayList<>();
        for (TaskFragment fragment : distributedExecutionPlan.getFragments()) {
            partialResults.add(fragmentTaskClient.execute(fragment.getWorkerAddress(), fragment));
        }
        return resultMerger.merge(logicalPlan, partialResults);
    }
}
