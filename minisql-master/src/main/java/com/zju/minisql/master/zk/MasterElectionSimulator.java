package com.zju.minisql.master.zk;

import java.lang.management.ManagementFactory;
import java.util.Random;

/**
 * 模拟 Master 节点启动与高可用选举 (Docker 适配版)
 */
public class MasterElectionSimulator {

    public static void main(String[] args) throws Exception {
        // 0. 强行关闭底层框架烦人的 Log4j 警告，保持答辩时控制台极致清爽
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

        // 1. 随机生成一个假端口，配合当前进程 PID，模拟不同的物理机器
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        int fakePort = 8000 + new Random().nextInt(1000);
        String myNodeAddress = "127.0.0.1:" + fakePort + " (PID:" + pid + ")";

        System.out.println("==================================================");
        System.out.println("👑 正在启动 Master 控制平面节点: " + myNodeAddress);
        System.out.println("==================================================");

        // 2. 动态读取 ZooKeeper 地址
        // 如果系统环境变量中存在 ZK_ADDR (Docker传入)，则使用环境变量；否则使用本地测试地址
        String zkAddress = System.getenv("ZK_ADDR") != null ? System.getenv("ZK_ADDR") : "127.0.0.1:2181";
        System.out.println("[环境配置] 目标 ZooKeeper 寻址: " + zkAddress);
        
        // 3. 实例化我们刚才写的协调器
        MasterCoordinator coordinator = new MasterCoordinator(zkAddress, myNodeAddress);
        
        // 4. 启动协调器（内部会开始竞选）
        coordinator.start();

        // 5. 阻塞主线程，防止进程直接退出 (模拟服务器长连接)
        System.out.println("\n[提示] 进程保持运行中... 等待集群心跳监听...");
        Thread.currentThread().join();
    }
}