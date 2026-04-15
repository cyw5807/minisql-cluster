package com.zju.minisql.master.zk;

import java.lang.management.ManagementFactory;
import java.util.Random;

/**
 * 模拟 Master 节点启动与高可用选举
 */
public class MasterElectionSimulator {

    public static void main(String[] args) throws Exception {
        // 1. 随机生成一个假端口，配合当前进程 PID，模拟不同的物理机器
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        int fakePort = 8000 + new Random().nextInt(1000);
        String myNodeAddress = "127.0.0.1:" + fakePort + " (PID:" + pid + ")";

        System.out.println("==================================================");
        System.out.println("正在启动 Master 节点: " + myNodeAddress);
        System.out.println("==================================================");

        // 2. 连接到本地的 ZooKeeper (请确保你的 ZK 服务端已在 2181 端口启动)
        String zkAddress = "127.0.0.1:2181";
        
        // 3. 实例化我们刚才写的协调器
        MasterCoordinator coordinator = new MasterCoordinator(zkAddress, myNodeAddress);
        
        // 4. 启动协调器（内部会开始竞选）
        coordinator.start();

        // 5. 阻塞主线程，防止进程直接退出 (模拟服务器长连接)
        System.out.println("\n[提示] 进程保持运行中... 可以在控制台按 Ctrl+C 或点击停止按钮杀死该进程。\n");
        Thread.currentThread().join();
    }
}