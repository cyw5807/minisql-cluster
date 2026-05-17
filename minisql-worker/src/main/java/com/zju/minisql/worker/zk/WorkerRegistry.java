package com.zju.minisql.worker.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * Worker 节点注册中心
 * 负责向 ZK 注册当前节点的存活状态，并提供断线重连自动恢复机制
 */
public class WorkerRegistry {

    private static final String WORKERS_ROOT_PATH = "/minisql/workers";
    private final CuratorFramework zkClient;

    public WorkerRegistry(String zkConnectString) {
        this.zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkConnectString)
                .sessionTimeoutMs(5000) // Worker 的心跳超时设置得短一些，以便 Master 快速感知宕机
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    public void register(String workerAddress) throws Exception {
        String path = WORKERS_ROOT_PATH + "/" + workerAddress;

        // ⭐ 核心修复：在 client 启动前，挂载连接状态监听器
        // 专门用于应对 docker pause / unpause 或网络抖动导致的会话过期断线重连场景
        zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                // 当底层网络与 ZK 重新建立连接时触发
                if (newState == ConnectionState.RECONNECTED) {
                    try {
                        // 检查原先的临时节点是否已被 ZK 服务器由于超时而无情擦除
                        if (client.checkExists().forPath(path) == null) {
                            // 如果丢失，立即重新创建，让集群控制面感知到算力平滑回归
                            client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
                            System.out.println("📡 [状态自愈] ZK 会话重连成功，已自动重新注册临时节点: " + path);
                        }
                    } catch (Exception e) {
                        System.err.println("❌ [状态自愈] 重新注册临时节点失败: " + e.getMessage());
                    }
                }
            }
        });

        // 启动 ZK 客户端
        zkClient.start();
        
        // 确保父目录存在
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(WORKERS_ROOT_PATH);
        }

        // 创建初始临时节点
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        }
        
        System.out.println("====== 本地 Worker 已成功注册到 ZK 集群 ======");
        System.out.println("注册路径: " + path);
    }
}