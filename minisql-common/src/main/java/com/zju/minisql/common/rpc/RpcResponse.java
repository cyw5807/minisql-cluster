package com.zju.minisql.common.rpc;

import java.io.Serializable;

/**
 * RPC 响应载体
 * 封装了跨网络执行的返回结果或异常信息
 */
public class RpcResponse implements Serializable {

    // 对应发送时的请求 ID，用于在异步 Map 中唤醒对应的 Future
    private long requestId;
    
    // 方法执行成功时的实际返回值 (例如查询到的 List<Record>)
    private Object result;
    
    // 方法执行失败时的错误信息堆栈 (成功时为 null)
    private String errorMessage;

    // Kryo 序列化框架必需的无参构造函数
    public RpcResponse() {
    }

    // --- 快速构造成功响应的静态工具方法 ---
    public static RpcResponse success(long requestId, Object result) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(requestId);
        response.setResult(result);
        return response;
    }

    // --- 快速构造失败响应的静态工具方法 ---
    public static RpcResponse fail(long requestId, String errorMessage) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(requestId);
        response.setErrorMessage(errorMessage);
        return response;
    }

    // --- 标准的 Getter 和 Setter ---
    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}