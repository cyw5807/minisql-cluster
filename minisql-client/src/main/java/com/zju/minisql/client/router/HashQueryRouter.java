package com.zju.minisql.client.router;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.query.model.FilterCondition;
import com.zju.minisql.client.planner.LogicalPlan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

/**
 * 基于主键等值条件的简化哈希路由器。
 * 当查询命中分片键等值谓词时，走单分片；否则广播到全部 Worker。
 */
public class HashQueryRouter implements QueryRouter {

    private final Supplier<List<String>> activeWorkerSupplier;

    public HashQueryRouter(Supplier<List<String>> activeWorkerSupplier) {
        this.activeWorkerSupplier = activeWorkerSupplier;
    }

    @Override
    public List<ExecutionTarget> route(LogicalPlan logicalPlan) {
        List<String> workers = new ArrayList<>(activeWorkerSupplier.get());
        workers.sort(Comparator.naturalOrder());
        if (workers.isEmpty()) {
            throw new IllegalStateException("当前没有可用的 Worker 节点");
        }

        if (logicalPlan.getQueryAst().hasJoin()) {
            return buildBroadcastTargets(workers);
        }

        FilterCondition filterCondition = logicalPlan.getQueryAst().getFilterCondition();
        ColumnMeta primaryKey = logicalPlan.getTableMeta().getPrimaryKey();
        if (primaryKey != null
                && filterCondition != null
                && filterCondition.getOperator() == FilterCondition.ComparisonOperator.EQ
                && primaryKey.getColumnName().equalsIgnoreCase(filterCondition.getColumnName())) {
            int partitionId = Math.floorMod(String.valueOf(filterCondition.getValue()).hashCode(), workers.size());
            return List.of(new ExecutionTarget(partitionId, workers.get(partitionId)));
        }

        return buildBroadcastTargets(workers);
    }

    private List<ExecutionTarget> buildBroadcastTargets(List<String> workers) {
        List<ExecutionTarget> targets = new ArrayList<>();
        for (int i = 0; i < workers.size(); i++) {
            targets.add(new ExecutionTarget(i, workers.get(i)));
        }
        return targets;
    }
}
