package com.zju.minisql.master.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 模拟 Master 节点的服务发现能力
 */
public class ClusterDiscoverySimulator {

    public static void main(String[] args) throws Exception {
        System.out.println("==================================================");
        System.out.println("🚀 正在启动 Master 节点的服务发现雷达...");
        System.out.println("==================================================");

        // 1. 连接本地 ZK
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        zkClient.start();

        // 2. 初始化并启动 Worker 监听
        WorkerDiscovery discovery = new WorkerDiscovery(zkClient);
        discovery.watchWorkers();

        System.out.println("\n[提示] Master 正在持续监听 Worker 的上下线事件...");
        System.out.println("[提示] 请保持本进程运行，并去启动 WorkerStarter 观察日志变化。\n");

        // 3. 阻塞主线程
        Thread.currentThread().join();
    }
}