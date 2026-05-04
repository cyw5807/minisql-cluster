package com.zju.minisql.master.planner;

import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.query.model.QueryAst;

/**
 * 逻辑计划。
 * 课程作业阶段先将逻辑计划简化为 AST 与表元数据的绑定结果。
 */
public class LogicalPlan {

    private final QueryAst queryAst;
    private final TableMeta tableMeta;
    private final TableMeta joinTableMeta;

    public LogicalPlan(QueryAst queryAst, TableMeta tableMeta) {
        this(queryAst, tableMeta, null);
    }

    public LogicalPlan(QueryAst queryAst, TableMeta tableMeta, TableMeta joinTableMeta) {
        this.queryAst = queryAst;
        this.tableMeta = tableMeta;
        this.joinTableMeta = joinTableMeta;
    }

    public QueryAst getQueryAst() {
        return queryAst;
    }

    public TableMeta getTableMeta() {
        return tableMeta;
    }

    public TableMeta getJoinTableMeta() {
        return joinTableMeta;
    }
}
