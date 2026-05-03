package com.zju.minisql.master.router;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.master.zk.WorkerDiscovery;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 全链路 RPC 路由与负载均衡联调测试
 */
public class FullLinkRouterSimulator {

    public static void main(String[] args) throws Exception {
        // 1. 连接 ZK 并初始化服务发现雷达
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        WorkerDiscovery discovery = new WorkerDiscovery(zkClient);
        discovery.watchWorkers();

        // 2. 初始化核心路由器
        TaskRouter router = new TaskRouter(discovery);

        // 给 Worker 留 3 秒钟的上线注册时间
        System.out.println("等待 Worker 节点就绪，3秒后开始全链路轰炸...");
        Thread.sleep(3000);

        // 3. 模拟连续发送 6 条 SQL 任务
        for (int i = 1; i <= 6; i++) {
            System.out.println("\n>>> [Master] 准备派发第 " + i + " 个任务...");
            
            // 构造底层的 RPC 请求体 (通过反射调用刚才定义的契约)
            RpcRequest request = new RpcRequest();
            request.setClassName("com.zju.minisql.common.service.SqlExecuteService");
            request.setMethodName("execute");
            request.setParameterTypes(new Class[]{String.class});
            request.setParameters(new Object[]{"SELECT * FROM table_" + i + " WHERE id = " + i + ";"});

            try {
                // 核心：调用 Router 进行负载均衡并发送
                Object result = router.routeAndExecute(request);
                System.out.println("<<< [Master] 收到远程返回结果: " + result);
            } catch (Exception e) {
                System.err.println("任务派发失败: " + e.getMessage());
            }

            // 稍微停顿一下，方便肉眼观察控制台
            Thread.sleep(1000);
        }
        
        System.out.println("\n全链路测试完毕！");
        System.exit(0);
    }
}