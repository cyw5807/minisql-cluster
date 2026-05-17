package com.zju.minisql.common.meta;

import com.zju.minisql.common.rpc.serialize.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群元数据管理器
 * 不仅负责将表结构信息 (Schema) 持久化，同时兼管分布式集群的 Worker 节点服务发现与动态感知。
 */
public class MetadataManager {

    // 严格遵循设计报告：持久化元数据根路径
    private static final String META_ROOT_PATH = "/minisql/metadata";
    
    // 🌟 新增：分布式计算集群 Worker 服务注册与发现的根路径
    private static final String WORKERS_ROOT_PATH = "/minisql/workers";
    
    private final CuratorFramework zkClient;
    private final Serializer serializer;

    public MetadataManager(CuratorFramework zkClient, Serializer serializer) {
        this.zkClient = zkClient;
        this.serializer = serializer;
    }

    /**
     * 初始化：确保所有核心 ZK 根路径均已存在
     */
    public void init() throws Exception {
        // 初始化 Schema 持久化路径
        if (zkClient.checkExists().forPath(META_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(META_ROOT_PATH);
            System.out.println("成功初始化元数据根目录: " + META_ROOT_PATH);
        }
        
        // 🌟 新增：初始化 Worker 注册中心路径
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(WORKERS_ROOT_PATH);
            System.out.println("成功初始化 Worker 发现中心根目录: " + WORKERS_ROOT_PATH);
        }
    }

    /**
     * 1. 创建表 (DDL: CREATE TABLE)
     */
    public void createTable(TableMeta tableMeta) throws Exception {
        String path = META_ROOT_PATH + "/" + tableMeta.getTableName();
        
        // 检查表是否已存在
        if (zkClient.checkExists().forPath(path) != null) {
            throw new RuntimeException("创建失败：表 [" + tableMeta.getTableName() + "] 已经存在！");
        }

        // 序列化为字节流并写入 ZK (持久化节点)
        byte[] data = serializer.serialize(tableMeta);
        zkClient.create().withMode(CreateMode.PERSISTENT).forPath(path, data);
        System.out.println("成功在 ZK 中持久化表结构: " + tableMeta.getTableName());
    }

    /**
     * 2. 获取表结构 (DQL: SELECT / 路由校验)
     */
    public TableMeta getTable(String tableName) throws Exception {
        String path = META_ROOT_PATH + "/" + tableName;
        
        if (zkClient.checkExists().forPath(path) == null) {
            System.err.println("警告：尝试获取不存在的表 [" + tableName + "]");
            return null;
        }

        // 从 ZK 读取字节流并反序列化
        byte[] data = zkClient.getData().forPath(path);
        return serializer.deserialize(data, TableMeta.class);
    }

    /**
     * 3. 删除表 (DDL: DROP TABLE)
     */
    public void dropTable(String tableName) throws Exception {
        String path = META_ROOT_PATH + "/" + tableName;
        
        if (zkClient.checkExists().forPath(path) != null) {
            zkClient.delete().forPath(path);
            System.out.println("成功从 ZK 中删除表结构: " + tableName);
        } else {
            throw new RuntimeException("删除失败：表 [" + tableName + "] 不存在！");
        }
    }

    /**
     * 4. 获取集群中所有的表名 (用于 SHOW TABLES 命令)
     */
    public List<String> getAllTableNames() throws Exception {
        if (zkClient.checkExists().forPath(META_ROOT_PATH) != null) {
            return zkClient.getChildren().forPath(META_ROOT_PATH);
        }
        return new ArrayList<>();
    }

    /**
     * 🌟 5. 新增：全动态获取当前时刻真正存活的 Worker 节点列表
     */
    public List<String> getActiveWorkers() throws Exception {
        if (zkClient.checkExists().forPath(WORKERS_ROOT_PATH) != null) {
            // 获取 ZK 的无序子节点列表
            List<String> workers = zkClient.getChildren().forPath(WORKERS_ROOT_PATH);
            
            // ⭐ 终极修复：强制按字典序排序！
            // 确保读路由和写路由拿到的永远是固定顺序 (如 9012, 9013, 9014)
            java.util.Collections.sort(workers);
            
            return workers;
        }
        return new ArrayList<>();
    }
}