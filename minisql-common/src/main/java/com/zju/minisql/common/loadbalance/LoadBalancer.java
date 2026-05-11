package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.NodeLoad;

import java.util.List;

public interface LoadBalancer {

    NodeInfo selectReadNode(int partitionId, List<NodeInfo> nodes);

    void reportNodeLoad(String nodeId, NodeLoad load);
}
