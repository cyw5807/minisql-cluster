package com.zju.minisql.worker.query;

import com.zju.minisql.common.query.executor.QueryFragmentExecutor;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.common.query.service.DistributedQueryTaskService;

/**
 * Worker 侧分布式查询服务实现。
 */
public class DistributedQueryTaskServiceImpl implements DistributedQueryTaskService {

    private final WorkerTableRepository repository;
    private final QueryFragmentExecutor queryFragmentExecutor = new QueryFragmentExecutor();

    public DistributedQueryTaskServiceImpl(WorkerTableRepository repository) {
        this.repository = repository;
    }

    @Override
    public PartialQueryResult executeFragment(TaskFragment fragment) {
        return queryFragmentExecutor.execute(
                fragment.getWorkerAddress(),
                fragment.getQueryAst(),
                repository::getTableRows,
                repository::insertRow,
                repository::deleteTable
        );
    }
}
