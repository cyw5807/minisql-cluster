package com.zju.minisql.common.query.service;

import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.TaskFragment;

/**
 * Worker 侧分布式查询子任务执行接口。
 */
public interface DistributedQueryTaskService {

    /**
     * 执行单个子任务，并返回局部结果。
     */
    PartialQueryResult executeFragment(TaskFragment fragment);
}
