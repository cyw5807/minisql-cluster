package com.zju.minisql.worker.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * Worker 节点注册中心
 * 负责向 ZK 注册当前节点的存活状态
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
        zkClient.start();
        
        // 确保父目录存在
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(WORKERS_ROOT_PATH);
        }

        // 创建临时节点
        String path = WORKERS_ROOT_PATH + "/" + workerAddress;
        zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        
        System.out.println("====== 本地 Worker 已成功注册到 ZK 集群 ======");
        System.out.println("注册路径: " + path);
    }
}