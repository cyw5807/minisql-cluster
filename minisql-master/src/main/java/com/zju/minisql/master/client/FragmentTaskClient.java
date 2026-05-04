package com.zju.minisql.master.client;

import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.TaskFragment;

/**
 * 子任务客户端抽象。
 */
public interface FragmentTaskClient {

    PartialQueryResult execute(String workerAddress, TaskFragment fragment);
}
