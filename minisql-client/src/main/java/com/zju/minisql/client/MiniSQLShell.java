package com.zju.minisql.client;

import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import com.zju.minisql.client.network.RpcFragmentTaskClient;

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
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

/**
 * MiniSQL 2.0 分布式数据库交互式命令行终端 (CLI) - 纯净动态版
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

            // 3. 初始化元数据中心 (纯净启动，不做任何硬编码预装载)
            MetadataManager metadataManager = new MetadataManager(zkClient, new KryoSerializer());
            metadataManager.init();

            // 4. 装配分布式协调器 (全链路大管家)
            DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(
                    metadataManager,
                    new JSqlParserSqlParser(),
                    new SimpleLogicalPlanner(new MetadataManagerTableMetadataProvider(metadataManager)),
                    new SimpleDistributedPlanGenerator(new HashQueryRouter(() -> {
                        List<String> workers = new ArrayList<>(workerDiscovery.getActiveWorkers());
                        workers.sort(Comparator.naturalOrder());
                        return workers;
                    })),
                    new RpcFragmentTaskClient(),
                    new ResultMerger()
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
                // 打印提示符：如果是新行打印 minisql>，如果是多行输入打印 ->
                if (sqlBuffer.length() == 0) {
                    System.out.print("minisql> ");
                } else {
                    System.out.print("      -> ");
                }

                String line = scanner.nextLine().trim();

                // 处理退出指令
                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit;") || line.equalsIgnoreCase("quit;")) {
                    System.out.println("Bye.");
                    break;
                }

                // 处理清空当前输入指令
                if (line.equalsIgnoreCase("\\c")) {
                    sqlBuffer.setLength(0);
                    continue;
                }

                if (line.isEmpty()) {
                    continue;
                }

                sqlBuffer.append(line).append(" ");

                // 只有当用户输入了分号 (;) 时，才认为一条 SQL 输入结束，开始执行
                if (line.endsWith(";")) {
                    // 去掉最后的分号，因为很多 Parser 遇到分号会解析失败
                    String finalSql = sqlBuffer.toString().trim();
                    finalSql = finalSql.substring(0, finalSql.length() - 1).trim();

                    try {
                        long startTime = System.currentTimeMillis();
                        
                        // ==========================================
                        // 🌟 核心执行与打印逻辑
                        // ==========================================
                        QueryResult result = coordinator.execute(finalSql);
                        
                        if (result != null) {
                            // 调用现成的 toPrettyString 打印精美表格
                            System.out.print(result.toPrettyString());
                            System.out.println(); // 强制加一个换行，让耗时统计另起一行
                        } else {
                            System.out.println("Query OK.");
                        }

                        long endTime = System.currentTimeMillis();
                        System.out.println("执行耗时: " + ((endTime - startTime) / 1000.0) + " sec\n");

                    } catch (Exception e) {
                        System.out.println("ERROR: SQL 执行失败 - " + e.getMessage() + "\n");
                    }

                    // 执行完毕后，清空 Buffer，准备迎接下一条 SQL
                    sqlBuffer.setLength(0);
                }
            }
            scanner.close();
            
        } finally {
            zkClient.close();
        }
    }
}