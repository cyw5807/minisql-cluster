package com.zju.minisql.common.cluster.meta;

import com.zju.minisql.common.cluster.NodeInfo;

import java.util.List;

/**
 * 分片元数据读写接口。
 */
public interface MetadataService {

    NodeInfo getPrimaryNode(int partitionId);

    List<NodeInfo> getAllReplicas(int partitionId);

    List<NodeInfo> getAllAliveNodes();

    void updatePartitionOwner(int partitionId, NodeInfo newPrimary);
}
