package com.zju.minisql.common.loadbalance;

import com.zju.minisql.common.cluster.NodeInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 平滑加权轮询。
 */
public class WeightedRoundRobin {

    private final Map<String, Integer> weights = new HashMap<>();
    private final Map<String, Integer> currentWeights = new HashMap<>();

    public synchronized NodeInfo select(List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes 不能为空");
        }
        int totalWeight = 0;
        NodeInfo bestNode = null;
        int bestWeight = Integer.MIN_VALUE;

        for (NodeInfo node : nodes) {
            String nodeId = node.getNodeId();
            int weight = weights.getOrDefault(nodeId, 1);
            int next = currentWeights.getOrDefault(nodeId, 0) + weight;
            currentWeights.put(nodeId, next);
            totalWeight += weight;

            if (next > bestWeight) {
                bestWeight = next;
                bestNode = node;
            }
        }

        if (bestNode == null) {
            return nodes.get(0);
        }
        String bestId = bestNode.getNodeId();
        currentWeights.put(bestId, currentWeights.getOrDefault(bestId, 0) - totalWeight);
        return bestNode;
    }

    public synchronized void updateWeight(String nodeId, int weight) {
        weights.put(nodeId, Math.max(1, weight));
    }
}
