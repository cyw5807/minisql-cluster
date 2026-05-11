package com.zju.minisql.common.replica;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.query.model.Row;

/**
 * 副本同步传输抽象，默认由上层接入 RPC。
 */
@FunctionalInterface
public interface ReplicaSyncTransport {

    boolean syncWrite(NodeInfo nodeInfo, int partitionId, Row row);
}
