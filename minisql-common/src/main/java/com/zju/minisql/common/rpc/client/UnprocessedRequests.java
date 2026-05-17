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
            // 因为引入了超时机制，迟到的回包会被路由到这里
            // 我们不能抛出异常导致 Netty 线程崩溃，而是打印警告并安全丢弃
            System.err.println("⚠️ 警告: 收到已超时或被丢弃的响应包, 已安全丢弃。RequestID: " + rpcResponse.getRequestId());
        }
    }

    // 3. 主动移除挂起的请求 (用于超时阻断等异常场景的内存清理)
    public void remove(long requestId) {
        UNPROCESSED_FUTURES.remove(requestId);
    }
}