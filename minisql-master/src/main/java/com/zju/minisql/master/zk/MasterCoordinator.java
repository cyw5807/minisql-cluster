package com.zju.minisql.master.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * Master 集群协调器 (高可用重构版)
 * 采用工业级 LeaderLatch 实现无缝选主与故障感知
 */
public class MasterCoordinator {

    private static final String MASTER_PATH = "/minisql/master";
    private static final String WORKERS_ROOT_PATH = "/minisql/workers";
    
    private final CuratorFramework zkClient;
    private final String currentNodeAddress; 
    
    // 🌟 新增：使用 Curator 官方的领导者门闩替换手写的 NodeCache
    private LeaderLatch leaderLatch;

    public MasterCoordinator(String zkConnectString, String currentNodeAddress) {
        this.currentNodeAddress = currentNodeAddress;
        this.zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkConnectString)
                .sessionTimeoutMs(30000) 
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    public void start() throws Exception {
        // 1. 启动 ZK 客户端
        zkClient.start();
        
        // 2. 初始化 Worker 注册的根路径
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(WORKERS_ROOT_PATH);
        }

        System.out.println("成功连接到 ZooKeeper，准备参与高可用 Leader 选举...");

        // 3. 🌟 初始化选主逻辑
        leaderLatch = new LeaderLatch(zkClient, MASTER_PATH, currentNodeAddress);

        // 4. 🌟 添加状态监听器（当状态发生真正改变时才会被回调，天然免疫网络抖动引起的误判）
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("👑 === 选举成功！当前节点已成为活跃 Master: " + currentNodeAddress + " ===");
                // TODO: 如果你有一些只有 Master 才能执行的定时任务（比如心跳检测、死节点清理），可以在这里启动
            }

            @Override
            public void notLeader() {
                System.out.println("⚠️ === 注意！当前节点失去 Leader 身份，已降级为备用 Master: " + currentNodeAddress + " ===");
                // TODO: 在这里暂停那些只有 Master 才能执行的任务
            }
        });

        // 5. 启动门闩（它会在后台静默排队抢锁，绝不抛出任何异常，也不会阻塞当前线程）
        leaderLatch.start();
        System.out.println("⏳ 当前节点已加入 HA 选举队列，正在监听主节点状态...");
    }

    /**
     * 判断当前节点是否为 Leader
     * 替换了原来的 isLeader 变量，直接向底层组件获取最新最准的权威状态！
     */
    public boolean isLeader() {
        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    /**
     * 💡 扩展防线：阻塞等待成为 Leader (可选使用)
     * 如果你的 Master 主程序希望“不是 Leader 就永远卡住不往下执行代码”，可以调用这个方法。
     */
    public void awaitLeadership() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.await();
        }
    }
    
    /**
     * 优雅停机清理
     */
    public void close() {
        try {
            if (leaderLatch != null) {
                leaderLatch.close();
            }
            if (zkClient != null) {
                zkClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}