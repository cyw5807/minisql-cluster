package com.zju.minisql.master.meta;

import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.rpc.serialize.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群元数据管理器
 * 负责将表结构信息 (Schema) 持久化到 ZooKeeper 中
 */
public class MetadataManager {

    // 严格遵循设计报告：持久化元数据根路径
    private static final String META_ROOT_PATH = "/minisql/metadata";
    
    private final CuratorFramework zkClient;
    private final Serializer serializer;

    public MetadataManager(CuratorFramework zkClient, Serializer serializer) {
        this.zkClient = zkClient;
        this.serializer = serializer;
    }

    /**
     * 初始化：确保根路径存在
     */
    public void init() throws Exception {
        if (zkClient.checkExists().forPath(META_ROOT_PATH) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(META_ROOT_PATH);
            System.out.println("成功初始化元数据根目录: " + META_ROOT_PATH);
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
}