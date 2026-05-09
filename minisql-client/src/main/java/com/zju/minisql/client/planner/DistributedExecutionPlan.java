package com.zju.minisql.client.planner;

import com.zju.minisql.common.query.model.TaskFragment;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式物理计划。
 */
public class DistributedExecutionPlan {

    private List<TaskFragment> fragments = new ArrayList<>();
    private boolean needCoordinatorMerge;

    public List<TaskFragment> getFragments() {
        return fragments;
    }

    public void setFragments(List<TaskFragment> fragments) {
        this.fragments = fragments;
    }

    public boolean isNeedCoordinatorMerge() {
        return needCoordinatorMerge;
    }

    public void setNeedCoordinatorMerge(boolean needCoordinatorMerge) {
        this.needCoordinatorMerge = needCoordinatorMerge;
    }
}
