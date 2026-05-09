package com.zju.minisql.client.network;

import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.common.query.service.DistributedQueryTaskService;
import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.client.NettyRpcClient;
import com.zju.minisql.client.network.FragmentTaskClient;

/**
 * 智能客户端专用的子任务执行器
 * 遵循 GFS 理念：客户端直接与 Worker 进行点对点（P2P）通信
 */
public class RpcFragmentTaskClient implements FragmentTaskClient {

    // 使用无参构造，复用底层 NettyRpcClient 的静态连接池和线程资源
    private final NettyRpcClient rpcClient = new NettyRpcClient();

    public PartialQueryResult execute(String workerAddress, TaskFragment fragment) {
        // 1. 解析地址
        String[] hostAndPort = workerAddress.split(":");
        String ip = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);

        // 2. 封装请求 (使用 long 型 requestId)
        RpcRequest request = new RpcRequest(
                DistributedQueryTaskService.class.getCanonicalName(),
                "executeFragment",
                new Class[]{TaskFragment.class},
                new Object[]{fragment}
        );

        // 3. P2P 同步调用：直接打向指定的 Worker
        RpcResponse response = rpcClient.sendRequestSync(request, ip, port);

        // 4. 异常处理：判断 errorMessage 是否存在
        if (response.getErrorMessage() != null) {
            throw new RuntimeException("Worker 节点任务执行失败: " + response.getErrorMessage());
        }

        return (PartialQueryResult) response.getResult();
    }
}