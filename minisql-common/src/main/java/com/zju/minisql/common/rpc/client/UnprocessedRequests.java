package com.zju.minisql.common.rpc.client;

import com.zju.minisql.common.rpc.RpcResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 存放未处理完毕的请求
 * Key: RequestID (long)
 * Value: 供业务线程阻塞等待的 CompletableFuture
 */
public class UnprocessedRequests {

    private static final ConcurrentHashMap<Long, CompletableFuture<RpcResponse>> UNPROCESSED_FUTURES = new ConcurrentHashMap<>();

    // 1. 发送请求前调用：将 Future 放入 Map
    public void put(long requestId, CompletableFuture<RpcResponse> future) {
        UNPROCESSED_FUTURES.put(requestId, future);
    }

    // 2. 接收到响应时调用：取出并移除 Future，将结果填入其中唤醒业务线程
    public void complete(RpcResponse rpcResponse) {
        CompletableFuture<RpcResponse> future = UNPROCESSED_FUTURES.remove(rpcResponse.getRequestId());
        if (future != null) {
            future.complete(rpcResponse);
        } else {
            throw new IllegalStateException("收到未知或已超时的响应, RequestID: " + rpcResponse.getRequestId());
        }
    }
}