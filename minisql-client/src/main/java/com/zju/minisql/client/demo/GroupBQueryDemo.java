package com.zju.minisql.client.demo;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import com.zju.minisql.client.network.RpcFragmentTaskClient;

// 【架构大挪移】：这些计算平面的类即将被我们移到 Client 模块
import com.zju.minisql.client.coordinator.DistributedQueryCoordinator;
import com.zju.minisql.client.merger.ResultMerger;
import com.zju.minisql.client.metadata.MetadataManagerTableMetadataProvider;
import com.zju.minisql.client.parser.JSqlParserSqlParser;
import com.zju.minisql.client.planner.SimpleDistributedPlanGenerator;
import com.zju.minisql.client.planner.SimpleLogicalPlanner;
import com.zju.minisql.client.router.HashQueryRouter;

// 【基建下沉】：这两位负责 ZK 的类即将被我们下沉到 Common 模块
import com.zju.minisql.common.zk.WorkerDiscovery;
import com.zju.minisql.common.meta.MetadataManager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 组员 B 查询引擎真实联调演示入口 (Smart Client 版)
 *
 * 运行前请先：
 * 1. 启动本地 ZooKeeper。
 * 2. 启动至少一个 WorkerStarter 进程。
 * 3. 再运行本类或配套脚本。
 */
public class GroupBQueryDemo {

    private static final String DEFAULT_ZK_ADDRESS = "127.0.0.1:2181";
    private static final int DEFAULT_EXPECTED_WORKERS = 2;

    public static void main(String[] args) throws Exception {
        String zkAddress = args.length > 0 ? args[0] : DEFAULT_ZK_ADDRESS;
        int expectedWorkers = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_EXPECTED_WORKERS;
        List<String> customSqls = args.length > 2
                ? Arrays.asList(Arrays.copyOfRange(args, 2, args.length))
                : List.of(
                        "SELECT name FROM student WHERE id = 1001",
                        "SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept",
                        "SELECT dept, AVG(score) AS avg_score FROM student GROUP BY dept",
                        "SELECT student.name, score.course FROM student JOIN score ON student.id = score.id"
                );

        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        zkClient.start();

        try {
            WorkerDiscovery workerDiscovery = new WorkerDiscovery(zkClient);
            workerDiscovery.watchWorkers();

            waitForWorkers(workerDiscovery, expectedWorkers);

            MetadataManager metadataManager = new MetadataManager(zkClient, new KryoSerializer());
            metadataManager.init();
            ensureDemoTables(metadataManager);

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

            System.out.println("========================================");
            System.out.println("组员 B 查询引擎联调演示开始 (Smart Client 节点)");
            System.out.println("ZooKeeper 地址: " + zkAddress);
            System.out.println("当前 Worker 列表: " + workerDiscovery.getActiveWorkers());
            System.out.println("========================================");

            for (String sql : customSqls) {
                System.out.println();
                System.out.println(">>> 执行 SQL: " + sql);
                QueryResult result = coordinator.execute(sql);
                System.out.println(result.toPrettyString());
            }
        } finally {
            zkClient.close();
        }
    }

    private static void waitForWorkers(WorkerDiscovery workerDiscovery, int expectedWorkers) throws InterruptedException {
        int retries = 20;
        while (retries-- > 0) {
            if (workerDiscovery.getActiveWorkers().size() >= expectedWorkers) {
                return;
            }
            Thread.sleep(1000);
        }
        if (workerDiscovery.getActiveWorkers().isEmpty()) {
            throw new IllegalStateException("未发现任何 Worker，请先启动 WorkerStarter");
        }
        System.out.println("提示：当前仅发现 " + workerDiscovery.getActiveWorkers().size()
                + " 个 Worker，少于期望值 " + expectedWorkers + "，演示将继续进行。");
    }

    private static void ensureDemoTables(MetadataManager metadataManager) throws Exception {
        if (!metadataManager.getAllTableNames().contains("student")) {
            TableMeta studentTable = new TableMeta("student");
            studentTable.addColumn(new ColumnMeta("id", "INT", 0, true, true));
            studentTable.addColumn(new ColumnMeta("name", "CHAR", 20, false, false));
            studentTable.addColumn(new ColumnMeta("dept", "CHAR", 20, false, false));
            studentTable.addColumn(new ColumnMeta("score", "INT", 0, false, false));
            metadataManager.createTable(studentTable);
            System.out.println("已自动初始化演示表 student 的元数据。");
        }

        if (!metadataManager.getAllTableNames().contains("score")) {
            TableMeta scoreTable = new TableMeta("score");
            scoreTable.addColumn(new ColumnMeta("id", "INT", 0, true, true));
            scoreTable.addColumn(new ColumnMeta("course", "CHAR", 32, false, false));
            scoreTable.addColumn(new ColumnMeta("grade", "INT", 0, false, false));
            metadataManager.createTable(scoreTable);
            System.out.println("已自动初始化演示表 score 的元数据。");
        }
    }
}