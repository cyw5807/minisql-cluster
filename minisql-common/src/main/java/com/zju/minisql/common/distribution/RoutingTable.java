package com.zju.minisql.common.distribution;

import com.zju.minisql.common.cluster.NodeInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 带 TTL 的路由缓存。
 */
public class RoutingTable {

    private static class CacheEntry {
        private final NodeInfo nodeInfo;
        private final long expireAt;

        private CacheEntry(NodeInfo nodeInfo, long expireAt) {
            this.nodeInfo = nodeInfo;
            this.expireAt = expireAt;
        }
    }

    private final long ttlMillis;
    private final Map<Integer, CacheEntry> cache = new ConcurrentHashMap<>();

    public RoutingTable(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    public NodeInfo get(int partitionId) {
        CacheEntry entry = cache.get(partitionId);
        if (entry == null) {
            return null;
        }
        if (System.currentTimeMillis() > entry.expireAt) {
            cache.remove(partitionId);
            return null;
        }
        return entry.nodeInfo;
    }

    public void put(int partitionId, NodeInfo nodeInfo) {
        cache.put(partitionId, new CacheEntry(nodeInfo, System.currentTimeMillis() + ttlMillis));
    }

    public void invalidate() {
        cache.clear();
    }
}
