package com.zju.minisql.common.cluster;

import java.io.Serializable;
import java.util.Objects;

/**
 * 集群节点信息。
 */
public class NodeInfo implements Serializable {

    private String nodeId;
    private String host;
    private int port;

    public NodeInfo() {
    }

    public NodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public static NodeInfo fromAddress(String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("非法节点地址: " + address);
        }
        return new NodeInfo(address, parts[0], Integer.parseInt(parts[1]));
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String address() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeInfo)) {
            return false;
        }
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(nodeId, nodeInfo.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "NodeInfo{"
                + "nodeId='" + nodeId + '\''
                + ", host='" + host + '\''
                + ", port=" + port
                + '}';
    }
}
