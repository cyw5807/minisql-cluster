package com.zju.minisql.common.distribution;

import com.zju.minisql.common.cluster.ClusterEvent;
import com.zju.minisql.common.cluster.NodeInfo;

import java.util.List;

public interface DistributionManager {

    NodeInfo routeForWrite(String key);

    NodeInfo routeForRead(String key);

    List<NodeInfo> getPartitionNodes(int partitionId);

    void onClusterChange(ClusterEvent event);
}
