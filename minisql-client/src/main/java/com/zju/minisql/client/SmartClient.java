package com.zju.minisql.client;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.client.NettyRpcClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GFS/BigTable 风格的智能客户端
 * 特性：绕过 Master 直连 Worker，本地缓存路由表
 */
public class SmartClient {

    private final CuratorFramework zkClient;
    private final NettyRpcClient rpcClient;
    
    // 本地路由缓存：TableName -> List of Worker Addresses (IP:Port)
    // 对应 BigTable 的 Metadata Cache
    private final Map<String, List<String>> routeCache = new ConcurrentHashMap<>();
    
    // 用于轮询负载均衡的计数器
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public SmartClient(String zkAddress) {
        // 1. 初始化 ZK 客户端，用于拉取元数据
        this.zkClient = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.zkClient.start();
        
        // 2. 初始化我们重构后的 Netty 发射器
        this.rpcClient = new NettyRpcClient();
    }

    /**
     * 执行 SQL 的核心逻辑
     */
    public void execute(String sql) throws Exception {
        System.out.println(">>> 正在处理 SQL: " + sql);
        
        // 1. 简单的表名解析（实际项目中这里会调用 JSqlParser）
        String tableName = parseTableName(sql);
        
        // 2. 获取路由信息 (GFS 寻址逻辑)
        List<String> workers = getRoutes(tableName);
        
        if (workers == null || workers.isEmpty()) {
            throw new RuntimeException("错误：找不到表 " + tableName + " 对应的数据节点");
        }

        // 3. 负载均衡：从可用的 Worker 中选出一个（Round-Robin）
        String targetWorker = selectWorker(workers);
        String[] parts = targetWorker.split(":");
        String ip = parts[0];
        int port = Integer.parseInt(parts[1]);

        // 4. 构建弹药 (RpcRequest)
        RpcRequest request = new RpcRequest(
                "com.zju.minisql.common.service.SqlExecuteService", 
                "execute", 
                new Class[]{String.class}, 
                new Object[]{sql}
        );

        // 5. P2P 发射：绕过 Master 直接打到 Worker 脸上
        System.out.println(">>> [寻址成功] 正在直接连接数据节点: " + targetWorker);
        rpcClient.sendRequestAsync(request, ip, port)
                .thenAccept(response -> {
                    System.out.println("<<< [执行结果]: " + response.getResult());
                })
                .exceptionally(ex -> {
                    System.err.println("!!! [通信失败]: " + ex.getMessage());
                    // 架构师注：这里可以加入 Cache 失效并重试 ZK 的逻辑
                    routeCache.remove(tableName);
                    return null;
                });
    }

    private List<String> getRoutes(String tableName) throws Exception {
        // 优先看缓存，没缓存再去问 ZK
        if (!routeCache.containsKey(tableName)) {
            System.out.println("--- [Cache Miss] 正在向 ZK 查询元数据 ---");
            String path = "/minisql/data_distribution/" + tableName;
            if (zkClient.checkExists().forPath(path) != null) {
                byte[] data = zkClient.getData().forPath(path);
                // 假设数据格式是 ip1:port1,ip2:port2
                List<String> nodes = List.of(new String(data).split(","));
                routeCache.put(tableName, nodes);
            }
        }
        return routeCache.get(tableName);
    }

    private String selectWorker(List<String> workers) {
        int index = Math.abs(roundRobinCounter.getAndIncrement()) % workers.size();
        return workers.get(index);
    }

    private String parseTableName(String sql) {
        // 简单的模拟解析，实际应使用组员 B 的解析器
        return "test_table";
    }
}