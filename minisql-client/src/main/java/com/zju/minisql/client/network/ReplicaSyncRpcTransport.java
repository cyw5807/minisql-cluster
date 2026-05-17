package com.zju.minisql.client.network;

import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.common.replica.ReplicaSyncTransport;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于现有 Fragment RPC 的副本同步传输实现。
 */
public class ReplicaSyncRpcTransport implements ReplicaSyncTransport {

    private final FragmentTaskClient fragmentTaskClient;

    public ReplicaSyncRpcTransport() {
        this(new RpcFragmentTaskClient());
    }

    public ReplicaSyncRpcTransport(FragmentTaskClient fragmentTaskClient) {
        this.fragmentTaskClient = fragmentTaskClient;
    }

    @Override
    public boolean syncWrite(NodeInfo nodeInfo, int partitionId, String tableName, Row row) {
        try {
            QueryAst insertAst = buildInsertAst(tableName, row);
            String fragmentId = "replica-sync-" + partitionId + "-" + System.nanoTime();
            fragmentTaskClient.execute(nodeInfo.address(), new TaskFragment(fragmentId, nodeInfo.address(), insertAst));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private QueryAst buildInsertAst(String tableName, Row row) {
        QueryAst ast = new QueryAst();
        ast.setStatementType("INSERT");
        ast.setTableName(tableName);
        ast.setProjectionColumns(new ArrayList<>(row.getValues().keySet()));

        List<Object> values = new ArrayList<>();
        for (Object value : row.getValues().values()) {
            values.add(value);
        }
        ast.setInsertValues(values);
        return ast;
    }
}
