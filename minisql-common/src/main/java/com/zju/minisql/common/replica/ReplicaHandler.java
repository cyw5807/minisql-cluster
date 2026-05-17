package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.loadbalance.LoadBalancer;

import java.util.List;

/**
 * 副本读取路由。
 */
public class ReplicaHandler {

    private final LoadBalancer loadBalancer;

    public ReplicaHandler(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    public NodeInfo selectReadNode(int partitionId, List<NodeInfo> nodes) {
        return loadBalancer.selectReadNode(partitionId, nodes);
    }
}
