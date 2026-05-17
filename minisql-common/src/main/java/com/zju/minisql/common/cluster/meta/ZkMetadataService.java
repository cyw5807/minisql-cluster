package com.zju.minisql.common.cluster.meta;

import com.zju.minisql.common.cluster.NodeInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 基于 ZooKeeper 的分片元数据服务。
 */
public class ZkMetadataService implements MetadataService {

    private static final String PARTITIONS_ROOT = "/minisql/partitions";
    private static final String WORKERS_ROOT = "/minisql/workers";

    private final CuratorFramework zkClient;

    public ZkMetadataService(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    public void init() {
        createPersistentIfAbsent(PARTITIONS_ROOT);
    }

    @Override
    public NodeInfo getPrimaryNode(int partitionId) {
        String path = partitionPath(partitionId) + "/primary";
        try {
            if (zkClient.checkExists().forPath(path) == null) {
                return null;
            }
            byte[] data = zkClient.getData().forPath(path);
            return decodeNode(data);
        } catch (Exception e) {
            throw new RuntimeException("读取主节点失败, partition=" + partitionId, e);
        }
    }

    @Override
    public List<NodeInfo> getAllReplicas(int partitionId) {
        String root = partitionPath(partitionId) + "/replicas";
        try {
            if (zkClient.checkExists().forPath(root) == null) {
                return Collections.emptyList();
            }
            List<NodeInfo> replicas = new ArrayList<>();
            for (String child : zkClient.getChildren().forPath(root)) {
                byte[] data = zkClient.getData().forPath(root + "/" + child);
                replicas.add(decodeNode(data));
            }
            return replicas;
        } catch (Exception e) {
            throw new RuntimeException("读取副本失败, partition=" + partitionId, e);
        }
    }

    @Override
    public List<NodeInfo> getAllAliveNodes() {
        try {
            if (zkClient.checkExists().forPath(WORKERS_ROOT) == null) {
                return Collections.emptyList();
            }
            List<NodeInfo> nodes = new ArrayList<>();
            for (String worker : zkClient.getChildren().forPath(WORKERS_ROOT)) {
                nodes.add(NodeInfo.fromAddress(worker));
            }
            return nodes;
        } catch (Exception e) {
            throw new RuntimeException("读取存活节点失败", e);
        }
    }

    @Override
    public void updatePartitionOwner(int partitionId, NodeInfo newPrimary) {
        String partitionRoot = partitionPath(partitionId);
        String primaryPath = partitionRoot + "/primary";
        createPersistentIfAbsent(partitionRoot);
        try {
            if (zkClient.checkExists().forPath(primaryPath) == null) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(primaryPath, encodeNode(newPrimary));
                return;
            }
            zkClient.setData().forPath(primaryPath, encodeNode(newPrimary));
        } catch (Exception e) {
            throw new RuntimeException("更新主节点失败, partition=" + partitionId, e);
        }
    }

    public void upsertReplicas(int partitionId, List<NodeInfo> replicas) {
        String root = partitionPath(partitionId) + "/replicas";
        createPersistentIfAbsent(partitionPath(partitionId));
        createPersistentIfAbsent(root);
        try {
            for (String child : zkClient.getChildren().forPath(root)) {
                zkClient.delete().forPath(root + "/" + child);
            }
            for (NodeInfo replica : replicas) {
                String nodePath = root + "/" + replica.getNodeId().replace(':', '_');
                try {
                    zkClient.create().withMode(CreateMode.PERSISTENT).forPath(nodePath, encodeNode(replica));
                } catch (KeeperException.NodeExistsException ignored) {
                    zkClient.setData().forPath(nodePath, encodeNode(replica));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("更新副本失败, partition=" + partitionId, e);
        }
    }

    private String partitionPath(int partitionId) {
        return PARTITIONS_ROOT + "/" + partitionId;
    }

    private void createPersistentIfAbsent(String path) {
        try {
            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            }
        } catch (KeeperException.NodeExistsException ignored) {
            // 并发创建时允许忽略
        } catch (Exception e) {
            throw new RuntimeException("创建元数据路径失败: " + path, e);
        }
    }

    private byte[] encodeNode(NodeInfo nodeInfo) {
        return (nodeInfo.getNodeId() + "|" + nodeInfo.getHost() + "|" + nodeInfo.getPort())
                .getBytes(StandardCharsets.UTF_8);
    }

    private NodeInfo decodeNode(byte[] data) {
        String[] parts = new String(data, StandardCharsets.UTF_8).split("\\|");
        if (parts.length != 3) {
            throw new IllegalArgumentException("非法节点编码");
        }
        return new NodeInfo(parts[0], parts[1], Integer.parseInt(parts[2]));
    }
}
