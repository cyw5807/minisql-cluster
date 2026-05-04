package com.zju.minisql.master.client;

import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.common.query.service.DistributedQueryTaskService;
import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.client.NettyRpcClient;

/**
 * 基于当前 Netty RPC 框架的子任务客户端。
 */
public class RpcFragmentTaskClient implements FragmentTaskClient {

    @Override
    public PartialQueryResult execute(String workerAddress, TaskFragment fragment) {
        String[] hostAndPort = workerAddress.split(":");
        NettyRpcClient rpcClient = new NettyRpcClient(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        RpcRequest request = new RpcRequest(
                DistributedQueryTaskService.class.getCanonicalName(),
                "executeFragment",
                new Class[]{TaskFragment.class},
                new Object[]{fragment}
        );
        RpcResponse response = rpcClient.sendRequest(request);
        if (!response.isSuccess()) {
            throw new RuntimeException("Worker 子任务执行失败", response.getError());
        }
        return (PartialQueryResult) response.getResult();
    }
}
