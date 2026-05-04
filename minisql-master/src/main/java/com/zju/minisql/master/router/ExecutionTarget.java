package com.zju.minisql.master.router;

/**
 * 单个子任务的执行目标。
 */
public class ExecutionTarget {

    private final int partitionId;
    private final String workerAddress;

    public ExecutionTarget(int partitionId, String workerAddress) {
        this.partitionId = partitionId;
        this.workerAddress = workerAddress;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }
}
