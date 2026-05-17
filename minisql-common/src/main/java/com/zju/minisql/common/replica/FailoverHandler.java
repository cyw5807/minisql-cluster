package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.cluster.meta.MetadataService;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Primary 故障切换处理。
 */
public class FailoverHandler {

    private final MetadataService metadataService;
    private final Map<String, Long> syncProgress = new ConcurrentHashMap<>();

    public FailoverHandler(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    public void recordSync(String nodeId, long index) {
        syncProgress.put(nodeId, index);
    }

    public NodeInfo electNewPrimary(int partitionId, List<NodeInfo> aliveReplicas) {
        NodeInfo candidate = aliveReplicas.stream()
                .max(Comparator.comparingLong(replica -> syncProgress.getOrDefault(replica.getNodeId(), 0L)))
                .orElse(null);
        if (candidate != null) {
            metadataService.updatePartitionOwner(partitionId, candidate);
        }
        return candidate;
    }
}
