package com.zju.minisql.client.router;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.query.model.FilterCondition;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.client.planner.LogicalPlan;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 查询路由测试。
 */
public class HashQueryRouterTest {

    @Test
    public void testPreciseRouteByPrimaryKey() {
        QueryAst queryAst = new QueryAst();
        queryAst.setTableName("student");
        queryAst.setProjectionColumns(List.of("name"));
        queryAst.setFilterCondition(new FilterCondition("id", FilterCondition.ComparisonOperator.EQ, 1001L));

        TableMeta tableMeta = tableMeta();
        HashQueryRouter router = new HashQueryRouter(() -> List.of("127.0.0.1:9011", "127.0.0.1:9012"));
        List<ExecutionTarget> targets = router.route(new LogicalPlan(queryAst, tableMeta));
        assertEquals(1, targets.size());
    }

    @Test
    public void testBroadcastWithoutShardKey() {
        QueryAst queryAst = new QueryAst();
        queryAst.setTableName("student");
        queryAst.setProjectionColumns(List.of("name"));
        queryAst.setFilterCondition(new FilterCondition("score", FilterCondition.ComparisonOperator.GTE, 90L));

        TableMeta tableMeta = tableMeta();
        HashQueryRouter router = new HashQueryRouter(() -> List.of("127.0.0.1:9011", "127.0.0.1:9012"));
        List<ExecutionTarget> targets = router.route(new LogicalPlan(queryAst, tableMeta));
        assertEquals(2, targets.size());
    }

    private TableMeta tableMeta() {
        TableMeta tableMeta = new TableMeta("student");
        tableMeta.addColumn(new ColumnMeta("id", "INT", 0, true, true));
        tableMeta.addColumn(new ColumnMeta("name", "CHAR", 20, false, false));
        tableMeta.addColumn(new ColumnMeta("dept", "CHAR", 20, false, false));
        tableMeta.addColumn(new ColumnMeta("score", "INT", 0, false, false));
        return tableMeta;
    }
}
