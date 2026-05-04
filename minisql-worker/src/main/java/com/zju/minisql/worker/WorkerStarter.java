package com.zju.minisql.worker;

import com.zju.minisql.common.rpc.server.NettyRpcServer;
import com.zju.minisql.common.rpc.server.ServiceProvider;
import com.zju.minisql.worker.query.DistributedQueryTaskServiceImpl;
import com.zju.minisql.worker.query.InMemoryTableRepository;
import com.zju.minisql.worker.service.MockSqlExecuteServiceImpl;
import com.zju.minisql.worker.zk.WorkerRegistry;

/**
 * Worker 节点启动入口
 */
public class WorkerStarter {

    public static void main(String[] args) throws Exception {
        // 实际部署时，端口可以通过 args[] 传入以支持单机启动多个 Worker
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9012;
        String workerAddress = "127.0.0.1:" + port;
        String zkAddress = "127.0.0.1:2181";

        // 1. 初始化本地服务提供者
        ServiceProvider serviceProvider = new ServiceProvider();
        
        // 注册旧的字符串 SQL 测试服务，兼容组长当前已有联调代码
        serviceProvider.registerService(new MockSqlExecuteServiceImpl());
        // 注册组员 B 新增的分布式子任务执行服务
        serviceProvider.registerService(new DistributedQueryTaskServiceImpl(InMemoryTableRepository.demoRepositoryFor(workerAddress)));
        System.out.println("本地服务初始化完成。");
        System.out.println("已加载演示数据，并注册分布式查询执行服务。");

        // 2. 向 ZooKeeper 注册当前 Worker 节点
        WorkerRegistry registry = new WorkerRegistry(zkAddress);
        registry.register(workerAddress);

        // 3. 启动 Netty RPC 服务端，开始监听 Master 发来的计算/存储请求
        NettyRpcServer rpcServer = new NettyRpcServer(serviceProvider);
        rpcServer.start(port); // start 方法会阻塞当前线程
    }
}
