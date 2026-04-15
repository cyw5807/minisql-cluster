package com.zju.minisql.master.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * Master 集群协调器
 * 负责与 ZooKeeper 交互，完成 Leader 选举与故障感知
 */
public class MasterCoordinator {

    private static final String MASTER_PATH = "/minisql/master";
    private static final String WORKERS_ROOT_PATH = "/minisql/workers";
    
    private final CuratorFramework zkClient;
    private final String currentNodeAddress; 
    private volatile boolean isLeader = false;
    
    // 将 NodeCache 提升为全局变量，确保只实例化一次
    private NodeCache nodeCache;

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
        zkClient.start();
        
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(WORKERS_ROOT_PATH);
        }

        // 在启动客户端时，就立刻初始化监听器，且仅执行一次
        initLeaderWatcher();

        System.out.println("成功连接到 ZooKeeper，开始尝试选举 Leader...");
        tryElectLeader();
    }

    /**
     * 初始化全局监听器
     */
    private void initLeaderWatcher() throws Exception {
        nodeCache = new NodeCache(zkClient, MASTER_PATH);
        nodeCache.getListenable().addListener(() -> {
            // 增加 !isLeader 判断。如果自己已经是 Leader，就忽略该事件，防止网络抖动导致的误判
            if (nodeCache.getCurrentData() == null && !isLeader) {
                System.out.println("!!! 警告: 检测到活跃 Master 宕机，备用节点立即发起重新选举 !!!");
                tryElectLeader();
            }
        });
        nodeCache.start();
    }

    private void tryElectLeader() {
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(MASTER_PATH, currentNodeAddress.getBytes());

            this.isLeader = true;
            System.out.println("=== 选举成功！当前节点已成为活跃 Master: " + currentNodeAddress + " ===");

        } catch (KeeperException.NodeExistsException e) {
            this.isLeader = false;
            try {
                byte[] leaderBytes = zkClient.getData().forPath(MASTER_PATH);
                System.out.println("=== 选举失败。当前集群已有活跃 Master: " + new String(leaderBytes) + " ===");
                System.out.println("本节点降级为备用 Master，正在监听 Leader 状态...");
            } catch (Exception ex) {
                System.out.println("=== 选举失败。且读取当前 Master 信息时发生异常 ===");
            }
            
        } catch (Exception e) {
            System.err.println("竞选 Leader 时发生未知异常");
            e.printStackTrace();
        }
    }

    public boolean isLeader() {
        return isLeader;
    }
}