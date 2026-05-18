package com.zju.minisql.client;

import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import com.zju.minisql.common.cluster.meta.ZkMetadataService;
import com.zju.minisql.common.cluster.ClusterEvent;
import com.zju.minisql.common.cluster.NodeInfo;
import com.zju.minisql.common.distribution.DistributionManager;
import com.zju.minisql.common.distribution.DistributionManagerImpl;
import com.zju.minisql.common.loadbalance.LoadBalancer;
import com.zju.minisql.common.loadbalance.LoadBalancerImpl;
import com.zju.minisql.common.replica.FailoverHandler;
import com.zju.minisql.common.replica.PrimaryHandler;
import com.zju.minisql.common.replica.ReplicaHandler;
import com.zju.minisql.common.replica.ReplicaManager;
import com.zju.minisql.client.network.RpcFragmentTaskClient;
import com.zju.minisql.client.network.ReplicaSyncRpcTransport;

import com.zju.minisql.client.coordinator.DistributedQueryCoordinator;
import com.zju.minisql.client.merger.ResultMerger;
import com.zju.minisql.client.metadata.MetadataManagerTableMetadataProvider;
import com.zju.minisql.client.parser.JSqlParserSqlParser;
import com.zju.minisql.client.planner.SimpleDistributedPlanGenerator;
import com.zju.minisql.client.planner.SimpleLogicalPlanner;
import com.zju.minisql.client.router.HashQueryRouter;

import com.zju.minisql.common.zk.WorkerDiscovery;
import com.zju.minisql.common.meta.MetadataManager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * MiniSQL 2.0 分布式数据库交互式命令行终端 (CLI) - 纯净动态版 (已支持高级多副本路由)
 */
public class MiniSQLShell {

    // 默认连接本地 ZK，如果 Docker 部署可以使用 args 传入环境变量
    private static final String DEFAULT_ZK_ADDRESS = "127.0.0.1:2181";

    public static void main(String[] args) throws Exception {
        // 0. 强行关闭底层框架烦人的 Log4j 警告，保持答辩时控制台极致清爽
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

        String zkAddress = System.getenv("ZK_ADDR") != null ? System.getenv("ZK_ADDR") : DEFAULT_ZK_ADDRESS;

        System.out.println("==================================================");
        System.out.println("🚀 正在初始化 MiniSQL Smart Client...");
        System.out.println("📡 目标 ZooKeeper 寻址: " + zkAddress);

        // 1. 初始化 ZK 客户端
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        zkClient.start();

        try {
            // 2. 启动服务发现 (动态获取 Worker 列表)
            WorkerDiscovery workerDiscovery = new WorkerDiscovery(zkClient);
            workerDiscovery.watchWorkers();
            
            // 稍微等待 1 秒，让 ZK 把 worker 列表推过来
            Thread.sleep(1000); 

            // 3. 初始化元数据中心
            MetadataManager metadataManager = new MetadataManager(zkClient, new KryoSerializer());
            metadataManager.init();

            ZkMetadataService zkMetadataService = new ZkMetadataService(zkClient);
            zkMetadataService.init();

            // =========================================================
            // 在协调器接管前，强行刷入 1024 个虚拟槽位的多副本拓扑结构
            // =========================================================
            System.out.println("⏳ [多副本强刷] 正在向 ZooKeeper 初始化 1024 个虚拟槽位的一致性哈希环...");
            List<String> currentWorkers = new ArrayList<>(workerDiscovery.getActiveWorkers());
            if (currentWorkers.isEmpty()) {
                // 兜底策略：如果启动过快 Worker 还没注册，先用默认的 3 个节点打底
                currentWorkers = Arrays.asList("127.0.0.1:9012", "127.0.0.1:9013", "127.0.0.1:9014");
                System.out.println("⚠️ 未检测到在线 Worker，使用默认节点兜底初始化槽位...");
            } else {
                currentWorkers.sort(Comparator.naturalOrder());
            }
            
            // 调用我们刚移植过来的槽位初始化方法
            initPartitionMetadata(zkMetadataService, currentWorkers);
            System.out.println("✅ [多副本强刷] 1024 个虚拟槽位 (主备关系) 元数据初始化成功！");
            // =========================================================

            DistributionManager distributionManager = new DistributionManagerImpl(zkMetadataService);
            LoadBalancer loadBalancer = new LoadBalancerImpl(zkMetadataService, distributionManager);
            ReplicaManager replicaManager = new com.zju.minisql.common.replica.ReplicaManagerImpl(
                    zkMetadataService,
                    new PrimaryHandler(new ReplicaSyncRpcTransport()),
                    new ReplicaHandler(loadBalancer),
                    new FailoverHandler(zkMetadataService)
            );

            workerDiscovery.onWorkerRemoved(worker -> {
                replicaManager.onNodeFailure(worker);
                distributionManager.onClusterChange(ClusterEvent.removed(NodeInfo.fromAddress(worker)));
            });
            workerDiscovery.onWorkerAdded(worker -> {
                replicaManager.onNodeRecovery(worker);
                distributionManager.onClusterChange(ClusterEvent.added(NodeInfo.fromAddress(worker)));
            });

            // 4. 装配分布式协调器 (全链路大管家，注入了 replicaManager 以开启副本写入分支)
            DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(
                    metadataManager,
                    new JSqlParserSqlParser(),
                    new SimpleLogicalPlanner(new MetadataManagerTableMetadataProvider(metadataManager)),
                    new SimpleDistributedPlanGenerator(new HashQueryRouter(() -> {
                        List<String> workers = new ArrayList<>(workerDiscovery.getActiveWorkers());
                        workers.sort(Comparator.naturalOrder());
                        return workers;
                    }, distributionManager)),
                    new RpcFragmentTaskClient(),
                    new ResultMerger(),
                    replicaManager
            );

            System.out.println("✅ 集群连接成功！当前发现活跃 Worker 节点数: " + workerDiscovery.getActiveWorkers().size());
            System.out.println("==================================================");
            System.out.println("  欢迎使用 MiniSQL 2.0 分布式终端");
            System.out.println("  Type 'exit' or 'quit' to quit. Type '\\c' to clear current buffer.");
            System.out.println("  注意：所有 SQL 语句必须以分号 (;) 结尾才能执行。");
            System.out.println("==================================================");

            // 5. 进入交互式读取循环
            Scanner scanner = new Scanner(System.in);
            StringBuilder sqlBuffer = new StringBuilder();

            while (true) {
                if (sqlBuffer.length() == 0) {
                    System.out.print("minisql> ");
                } else {
                    System.out.print("      -> ");
                }

                String line = scanner.nextLine().trim();

                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit;") || line.equalsIgnoreCase("quit;")) {
                    System.out.println("Bye.");
                    break;
                }

                if (line.equalsIgnoreCase("\\c")) {
                    sqlBuffer.setLength(0);
                    continue;
                }

                if (line.isEmpty()) {
                    continue;
                }

                sqlBuffer.append(line).append(" ");

                if (line.endsWith(";")) {
                    String finalSql = sqlBuffer.toString().trim();
                    finalSql = finalSql.substring(0, finalSql.length() - 1).trim();

                    try {
                        long startTime = System.currentTimeMillis();
                        
                        QueryResult result = coordinator.execute(finalSql);
                        
                        if (result != null) {
                            System.out.print(result.toPrettyString());
                            System.out.println();
                        } else {
                            System.out.println("Query OK.");
                        }

                        long endTime = System.currentTimeMillis();
                        System.out.println("执行耗时: " + ((endTime - startTime) / 1000.0) + " sec\n");

                    } catch (Exception e) {
                        System.out.println("ERROR: SQL 执行失败 - " + e.getMessage() + "\n");
                        // 如果有复杂错，可以把这行打开： e.printStackTrace();
                    }

                    sqlBuffer.setLength(0);
                }
            }
            scanner.close();
            
        } finally {
            zkClient.close();
        }
    }

    /**
     * 在 ZK 中写入 1024 个槽位，并将它们分配给当前在线的 Worker 节点，同时计算出主备关系。
     */
    private static void initPartitionMetadata(ZkMetadataService metadataService, List<String> workers) {
        if (workers.isEmpty()) {
            return;
        }
        List<NodeInfo> nodes = workers.stream().map(NodeInfo::fromAddress).collect(Collectors.toList());
        for (int partitionId = 0; partitionId < 1024; partitionId++) {
            // 选择主节点
            NodeInfo primary = nodes.get(partitionId % nodes.size());
            metadataService.updatePartitionOwner(partitionId, primary);
            // 选择除主节点外的其他节点作为备用副本
            List<NodeInfo> replicas = nodes.stream()
                    .filter(node -> !node.getNodeId().equals(primary.getNodeId()))
                    .collect(Collectors.toList());
            metadataService.upsertReplicas(partitionId, replicas);
        }
    }
}