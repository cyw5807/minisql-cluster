package com.zju.minisql.common.rpc;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RPC 请求载体
 * 封装了跨网络反射调用所需的全部信息
 */
public class RpcRequest implements Serializable {

    // 用于模拟雪花算法的简单自增生成器（保证本地多线程下的唯一性）
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    // 全局唯一的请求 ID (改为 long 类型，严格占用 8 字节)
    private long requestId;
    // 需要调用的目标接口全限定名 (例如: com.zju.minisql.common.service.StorageService)
    private String className;
    // 需要调用的具体方法名 (例如: fetchPartition)
    private String methodName;
    // 方法的参数类型列表，用于精确匹配重载方法
    private Class<?>[] parameterTypes;
    // 实际传入的参数值
    private Object[] parameters;

    // Kryo 序列化框架必需的无参构造函数
    public RpcRequest() {
    }

    // 为了方便快速构建请求对象提供的构造函数
    public RpcRequest(String className, String methodName, Class<?>[] parameterTypes, Object[] parameters) {
        // 自动生成 long 类型的 ID，避免 UUID 带来的字符串开销
        this.requestId = ID_GENERATOR.incrementAndGet(); 
        this.className = className;
        this.methodName = methodName;
        this.parameterTypes = parameterTypes;
        this.parameters = parameters;
    }

    // --- 以下为标准的 Getter 和 Setter (已将 requestId 改为 long) ---
    public long getRequestId() { return requestId; }
    public void setRequestId(long requestId) { this.requestId = requestId; }
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }
    public Class<?>[] getParameterTypes() { return parameterTypes; }
    public void setParameterTypes(Class<?>[] parameterTypes) { this.parameterTypes = parameterTypes; }
    public Object[] getParameters() { return parameters; }
    public void setParameters(Object[] parameters) { this.parameters = parameters; }
}