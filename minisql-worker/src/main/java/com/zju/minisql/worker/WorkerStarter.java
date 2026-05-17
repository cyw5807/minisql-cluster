package com.zju.minisql.worker;

import com.zju.minisql.common.rpc.server.NettyRpcServer;
import com.zju.minisql.common.rpc.server.ServiceProvider;
import com.zju.minisql.worker.query.DistributedQueryTaskServiceImpl;
import com.zju.minisql.worker.query.InMemoryTableRepository;
import com.zju.minisql.worker.service.MockSqlExecuteServiceImpl;
import com.zju.minisql.worker.storage.LocalStorageEngine;
import com.zju.minisql.worker.storage.LocalStorageEngineImpl;
import com.zju.minisql.worker.storage.model.Row;
import com.zju.minisql.worker.zk.WorkerRegistry;

import java.nio.file.Path;
import java.util.HashMap;

/**
 * Worker 节点启动入口 (Docker 适配版)
 */
public class WorkerStarter {

    public static void main(String[] args) throws Exception {
        // 0. 强行关闭底层框架烦人的 Log4j 警告，保持答辩时控制台极致清爽
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

        // 1. 动态获取端口和地址配置
        // 实际部署时，端口可以通过 args[] 传入以支持单机启动多个 Worker
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9012;
        
        // 注意：这里保留 127.0.0.1 是一个非常聪明的"网络穿透"技巧。
        // 因为我们在 Docker 部署时会使用端口映射 (如 9012:9012)，
        // 并且演示时 Client 是运行在宿主机上的。Worker 告诉 ZK 自己在 127.0.0.1:9012，
        // 宿主机的 Client 就能顺利通过这个映射地址把 RPC 请求打进 Docker 容器内部！
        String workerAddress = "127.0.0.1:" + port; 
        
        // 动态读取 ZooKeeper 地址
        String zkAddress = System.getenv("ZK_ADDR") != null ? System.getenv("ZK_ADDR") : "127.0.0.1:2181";

        System.out.println("==================================================");
        System.out.println("🚀 正在启动 Worker 计算/存储节点 | 监听端口: " + port);
        System.out.println("📡 目标 ZooKeeper 寻址: " + zkAddress);
        System.out.println("==================================================");

        // 2. 初始化本地服务提供者
        ServiceProvider serviceProvider = new ServiceProvider();
        
        // 注册旧的字符串 SQL 测试服务，兼容组长当前已有联调代码
        serviceProvider.registerService(new MockSqlExecuteServiceImpl());
        // 注册组员 B 新增的分布式子任务执行服务
        serviceProvider.registerService(new DistributedQueryTaskServiceImpl(InMemoryTableRepository.demoRepositoryFor(workerAddress)));

        // 初始化 A 组本地存储引擎，作为副本与迁移模块的数据承载层。
        LocalStorageEngine storageEngine = new LocalStorageEngineImpl(Path.of("worker-data", "port-" + port));
        Row bootRow = new Row("boot", "system", 0, new HashMap<>());
        bootRow.getColumns().put("status", "ready");
        storageEngine.insert("system", bootRow);
        System.out.println("本地服务初始化完成。");
        System.out.println("已加载演示数据，并注册分布式查询执行服务。");
        System.out.println("本地存储引擎初始化完成，启动记录: " + storageEngine.get("system", "boot"));

        // 3. 向 ZooKeeper 注册当前 Worker 节点
        WorkerRegistry registry = new WorkerRegistry(zkAddress);
        registry.register(workerAddress);
        System.out.println("✅ 成功向 ZooKeeper 注册自身路由地址: " + workerAddress);

        // 4. 启动 Netty RPC 服务端，开始监听 Master 发来的计算/存储请求
        System.out.println("⏳ Netty RPC 神经末梢已就绪，开始阻塞监听流量...");
        NettyRpcServer rpcServer = new NettyRpcServer(serviceProvider);
        rpcServer.start(port); // start 方法会阻塞当前线程
    }
}