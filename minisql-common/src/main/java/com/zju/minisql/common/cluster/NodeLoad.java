package com.zju.minisql.common.cluster;

/**
 * 节点负载快照。
 */
public class NodeLoad {

    private final String nodeId;
    private final long dataSize;
    private final long capacity;
    private final double qps;

    public NodeLoad(String nodeId, long dataSize, long capacity, double qps) {
        this.nodeId = nodeId;
        this.dataSize = dataSize;
        this.capacity = capacity;
        this.qps = qps;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getDataSize() {
        return dataSize;
    }

    public long getCapacity() {
        return capacity;
    }

    public double getQps() {
        return qps;
    }

    public double usage() {
        if (capacity <= 0) {
            return 0D;
        }
        return (double) dataSize / (double) capacity;
    }
}
