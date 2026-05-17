package com.zju.minisql.common.distribution;

import com.zju.minisql.common.cluster.NodeInfo;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * 一致性哈希环，支持虚拟节点。
 */
public class ConsistentHashRing {

    private final int virtualNodeCount;
    private final TreeMap<Long, NodeInfo> ring = new TreeMap<>();

    public ConsistentHashRing(int virtualNodeCount) {
        this.virtualNodeCount = virtualNodeCount;
    }

    public synchronized void addNode(NodeInfo node) {
        for (int i = 0; i < virtualNodeCount; i++) {
            long hash = hash(node.getNodeId() + "#" + i);
            ring.put(hash, node);
        }
    }

    public synchronized void removeNode(NodeInfo node) {
        for (int i = 0; i < virtualNodeCount; i++) {
            long hash = hash(node.getNodeId() + "#" + i);
            ring.remove(hash);
        }
    }

    public synchronized void rebuild(Collection<NodeInfo> nodes) {
        ring.clear();
        for (NodeInfo node : nodes) {
            addNode(node);
        }
    }

    public synchronized NodeInfo getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = hash(key);
        SortedMap<Long, NodeInfo> tailMap = ring.tailMap(hash);
        Long target = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        return ring.get(target);
    }

    public synchronized boolean isEmpty() {
        return ring.isEmpty();
    }

    public synchronized int ringSize() {
        return ring.size();
    }

    private long hash(String raw) {
        CRC32 crc32 = new CRC32();
        crc32.update(raw.getBytes(StandardCharsets.UTF_8));
        return crc32.getValue();
    }

    public synchronized TreeMap<Long, NodeInfo> snapshot() {
        return new TreeMap<>(ring);
    }
}
