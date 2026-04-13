package com.zju.minisql.common.rpc.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地服务注册中心
 * 存放 接口全限定名 -> 具体实现类实例 的映射
 */
public class ServiceProvider {

    // 存放服务的 Map
    private final Map<String, Object> serviceMap = new ConcurrentHashMap<>();

    /**
     * 注册服务
     * @param service 具体的服务实现类实例
     */
    public void registerService(Object service) {
        // 获取该实现类实现的所有接口
        Class<?>[] interfaces = service.getClass().getInterfaces();
        if (interfaces.length == 0) {
            throw new RuntimeException("服务未实现任何接口");
        }
        // 默认将服务绑定到它实现的第一个接口上（简化设计）
        String interfaceName = interfaces[0].getCanonicalName();
        serviceMap.put(interfaceName, service);
        System.out.println("成功注册服务: " + interfaceName);
    }

    /**
     * 获取服务实例
     */
    public Object getService(String interfaceName) {
        Object service = serviceMap.get(interfaceName);
        if (service == null) {
            throw new RuntimeException("找不到对应的服务实现: " + interfaceName);
        }
        return service;
    }
}