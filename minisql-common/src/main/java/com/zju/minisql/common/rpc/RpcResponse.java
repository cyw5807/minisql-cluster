package com.zju.minisql.common.rpc;

import java.io.Serializable;

/**
 * RPC 响应载体
 * 封装了方法执行的结果或抛出的异常
 */
public class RpcResponse implements Serializable {

    // 对应请求的 ID (改为 long 类型，严格占用 8 字节)
    private long requestId;
    // 抛出的异常信息（如果执行成功，此项为 null）
    private Throwable error;
    // 方法执行的实际返回结果（如果执行失败，此项为 null）
    private Object result;

    // Kryo 序列化框架必需的无参构造函数
    public RpcResponse() {
    }

    // 静态工厂方法：构建成功响应 (注意：入参 requestId 已改为 long)
    public static RpcResponse success(long requestId, Object result) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(requestId);
        response.setResult(result);
        return response;
    }

    // 静态工厂方法：构建失败响应 (注意：入参 requestId 已改为 long)
    public static RpcResponse fail(long requestId, Throwable error) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(requestId);
        response.setError(error);
        return response;
    }

    // 判断本次 RPC 调用是否成功
    public boolean isSuccess() {
        return this.error == null;
    }

    // --- 以下为标准的 Getter 和 Setter (已将 requestId 改为 long) ---
    public long getRequestId() { return requestId; }
    public void setRequestId(long requestId) { this.requestId = requestId; }
    public Throwable getError() { return error; }
    public void setError(Throwable error) { this.error = error; }
    public Object getResult() { return result; }
    public void setResult(Object result) { this.result = result; }
}