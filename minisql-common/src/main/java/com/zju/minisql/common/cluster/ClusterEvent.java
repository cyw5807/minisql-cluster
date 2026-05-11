package com.zju.minisql.common.cluster;

/**
 * 集群节点变更事件。
 */
public class ClusterEvent {

    public enum Type {
        NODE_ADDED,
        NODE_REMOVED,
        FULL_REFRESH
    }

    private final Type type;
    private final NodeInfo nodeInfo;

    public ClusterEvent(Type type, NodeInfo nodeInfo) {
        this.type = type;
        this.nodeInfo = nodeInfo;
    }

    public Type getType() {
        return type;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public static ClusterEvent added(NodeInfo nodeInfo) {
        return new ClusterEvent(Type.NODE_ADDED, nodeInfo);
    }

    public static ClusterEvent removed(NodeInfo nodeInfo) {
        return new ClusterEvent(Type.NODE_REMOVED, nodeInfo);
    }

    public static ClusterEvent refresh() {
        return new ClusterEvent(Type.FULL_REFRESH, null);
    }
}
