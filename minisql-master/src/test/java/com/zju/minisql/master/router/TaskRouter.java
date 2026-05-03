package com.zju.minisql.master.router;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.client.NettyRpcClient;
import com.zju.minisql.master.zk.WorkerDiscovery;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 全局任务路由与调度器
 * 负责将计算/存储任务负载均衡地分发给底层的 Worker 集群
 */
public class TaskRouter {

    private final WorkerDiscovery workerDiscovery;
    
    // 使用 AtomicInteger 保证多线程并发提交任务时，轮询算法依然绝对均匀
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public TaskRouter(WorkerDiscovery workerDiscovery) {
        this.workerDiscovery = workerDiscovery;
    }

    /**
     * 核心调度方法：接收 RPC 请求，选择节点并同步返回执行结果
     */
    public Object routeAndExecute(RpcRequest request) {
        // 1. 获取当前集群中所有活着的 Worker
        List<String> activeWorkers = workerDiscovery.getActiveWorkers();
        
        if (activeWorkers == null || activeWorkers.isEmpty()) {
            throw new RuntimeException("致命错误：集群中没有任何存活的 Worker 节点，任务调度失败！");
        }

        // 2. 负载均衡：使用轮询 (Round-Robin) 算法挑选目标节点
        // 使用 Math.abs 防止整数溢出变成负数导致数组越界
        int index = Math.abs(roundRobinCounter.getAndIncrement()) % activeWorkers.size();
        String targetWorker = activeWorkers.get(index);

        System.out.println("-> [调度器] 准备就绪，当前存活节点数: " + activeWorkers.size() + "，选中目标: " + targetWorker);

        // 3. 解析目标的 IP 和 端口
        String[] hostAndPort = targetWorker.split(":");
        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);

        // 4. 发起底层的 Netty 网络通信
        // （架构师注：为了当前阶段快速跑通，我们每次新建一个 Client。在生产环境或后续优化中，这里应当重用一个 Channel 连接池）
        NettyRpcClient rpcClient = new NettyRpcClient(host, port);
        
        System.out.println("-> [网络层] 正在向 " + targetWorker + " 发送执行任务: " + request.getMethodName());
        RpcResponse response = rpcClient.sendRequest(request);

        // 5. 处理远程返回的结果
        if (response.isSuccess()) {
            return response.getResult();
        } else {
            // 如果 Worker 执行过程中抛出了异常（比如 SQL 语法错误、主键冲突），在 Master 侧还原并抛出
            throw new RuntimeException("远程 Worker 节点执行任务失败", response.getError());
        }
    }
}