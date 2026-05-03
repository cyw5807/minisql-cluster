package com.zju.minisql.master.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 集群存活节点发现机制
 * 供 Master 使用，实时维护当前可用的 Worker 列表
 */
public class WorkerDiscovery {

    private static final String WORKERS_ROOT_PATH = "/minisql/workers";
    private final CuratorFramework zkClient;
    
    // 使用线程安全的集合来保存当前活着的 Worker 地址 (如 ["127.0.0.1:9001", "127.0.0.1:9002"])
    private final List<String> activeWorkers = new CopyOnWriteArrayList<>();

    public WorkerDiscovery(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    public void watchWorkers() throws Exception {
        // 1. 获取初始可用列表
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) != null) {
            activeWorkers.addAll(zkClient.getChildren().forPath(WORKERS_ROOT_PATH));
            System.out.println("初始化可用 Worker 列表: " + activeWorkers);
        }

        // 2. 开启子节点变化监听
        PathChildrenCache cache = new PathChildrenCache(zkClient, WORKERS_ROOT_PATH, true);
        cache.getListenable().addListener((client, event) -> {
            String path = event.getData() != null ? event.getData().getPath() : "";
            // 从完整路径中提取出 IP:PORT
            String workerAddress = path.replace(WORKERS_ROOT_PATH + "/", "");

            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                activeWorkers.add(workerAddress);
                System.out.println("✨ [动态扩容] 新 Worker 上线: " + workerAddress + "，当前可用列表: " + activeWorkers);
            } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                activeWorkers.remove(workerAddress);
                System.out.println("⚠️ [故障感知] Worker 下线: " + workerAddress + "，当前可用列表: " + activeWorkers);
            }
        });
        
        // NORMAL 模式表示启动时不仅获取当前状态，还会触发一次现存节点的 ADDED 事件
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    }

    /**
     * 供后续的 SQL 路由策略 (如轮询、哈希) 获取当前可用机器
     */
    public List<String> getActiveWorkers() {
        return activeWorkers;
    }
}