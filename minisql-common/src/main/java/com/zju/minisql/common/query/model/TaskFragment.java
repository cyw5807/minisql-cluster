package com.zju.minisql.common.query.model;

import java.io.Serializable;

/**
 * 下发到单个 Worker 的子任务。
 */
public class TaskFragment implements Serializable {

    private String fragmentId;
    private String workerAddress;
    private QueryAst queryAst;

    public TaskFragment() {
    }

    public TaskFragment(String fragmentId, String workerAddress, QueryAst queryAst) {
        this.fragmentId = fragmentId;
        this.workerAddress = workerAddress;
        this.queryAst = queryAst;
    }

    public String getFragmentId() {
        return fragmentId;
    }

    public void setFragmentId(String fragmentId) {
        this.fragmentId = fragmentId;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public void setWorkerAddress(String workerAddress) {
        this.workerAddress = workerAddress;
    }

    public QueryAst getQueryAst() {
        return queryAst;
    }

    public void setQueryAst(QueryAst queryAst) {
        this.queryAst = queryAst;
    }
}
