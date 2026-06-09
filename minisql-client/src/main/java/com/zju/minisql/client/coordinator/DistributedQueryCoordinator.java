package com.zju.minisql.client.coordinator;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.MetadataManager;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.query.model.Row;
import com.zju.minisql.common.query.model.PartialQueryResult;
import com.zju.minisql.common.query.model.QueryAst;
import com.zju.minisql.common.query.model.QueryResult;
import com.zju.minisql.common.query.model.TaskFragment;
import com.zju.minisql.common.replica.ReplicaManager;
import com.zju.minisql.common.replica.ReplicaResult;
import com.zju.minisql.client.network.FragmentTaskClient;
import com.zju.minisql.client.merger.ResultMerger;
import com.zju.minisql.client.parser.SqlParser;
import com.zju.minisql.client.planner.DistributedExecutionPlan;
import com.zju.minisql.client.planner.DistributedPlanGenerator;
import com.zju.minisql.client.planner.LogicalPlan;
import com.zju.minisql.client.planner.LogicalPlanner;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式查询协调器。
 * 负责串联“解析 -> 计划 -> 路由 -> 下发 -> 汇总”完整流程。
 */
public class DistributedQueryCoordinator {

    private final MetadataManager metadataManager;
    private final SqlParser sqlParser;
    private final LogicalPlanner logicalPlanner;
    private final DistributedPlanGenerator distributedPlanGenerator;
    private final FragmentTaskClient fragmentTaskClient;
    private final ResultMerger resultMerger;
    private final ReplicaManager replicaManager;

    public DistributedQueryCoordinator(MetadataManager metadataManager,
                                       SqlParser sqlParser,
                                       LogicalPlanner logicalPlanner,
                                       DistributedPlanGenerator distributedPlanGenerator,
                                       FragmentTaskClient fragmentTaskClient,
                                       ResultMerger resultMerger) {
        this(metadataManager, sqlParser, logicalPlanner, distributedPlanGenerator, fragmentTaskClient, resultMerger, null);
    }

    public DistributedQueryCoordinator(MetadataManager metadataManager,
                                       SqlParser sqlParser,
                                       LogicalPlanner logicalPlanner,
                                       DistributedPlanGenerator distributedPlanGenerator,
                                       FragmentTaskClient fragmentTaskClient,
                                       ResultMerger resultMerger,
                                       ReplicaManager replicaManager) {
        this.metadataManager = metadataManager;
        this.sqlParser = sqlParser;
        this.logicalPlanner = logicalPlanner;
        this.distributedPlanGenerator = distributedPlanGenerator;
        this.fragmentTaskClient = fragmentTaskClient;
        this.resultMerger = resultMerger;
        this.replicaManager = replicaManager;
    }

    /**
     * 分布式协调器全链路入口
     * 严格践行“控制流与数据流分离”架构
     */
    public QueryResult execute(String sql) throws Exception {
        // 1. 词法与语法解析生成 AST
        QueryAst ast = sqlParser.parse(sql);

        // ==========================================
        // 🌟 阶段一：控制流拦截 (DDL - 建表/删表)
        // 绕过 Planner 和 Worker，由 Master 控制面直接写入 ZK 元数据
        // ==========================================
        if ("CREATE_TABLE".equals(ast.getStatementType())) {
            // 防止重名表冲突
            if (metadataManager.getAllTableNames().contains(ast.getTableName())) {
                throw new RuntimeException("表已存在: " + ast.getTableName());
            }

            TableMeta tableMeta = new TableMeta(ast.getTableName());
            // 将 AST 解析出的纯净列定义，转换为底层存储需要的 ColumnMeta
            for (QueryAst.ColumnDef colDef : ast.getColumnDefinitions()) {
                // 演示环境的简化逻辑：如果字段名叫 id，我们默认把它当成主键和索引
                boolean isPk = colDef.getName().equalsIgnoreCase("id");
                tableMeta.addColumn(new ColumnMeta(colDef.getName(), colDef.getDataType(), 0, isPk, isPk));
            }
            
            // 直接操作 ZK 持久化元数据！
            metadataManager.createTable(tableMeta); 
            return null; // DDL 属于控制指令，没有结果集返回
        }

        if ("DROP_TABLE".equals(ast.getStatementType())) {
            metadataManager.dropTable(ast.getTableName());
            List<String> activeWorkers = metadataManager.getActiveWorkers();
            for (String workerAddress : activeWorkers) {
                TaskFragment fragment = new TaskFragment(
                        "drop-" + ast.getTableName() + "-" + System.nanoTime(),
                        workerAddress,
                        ast
                );
                fragmentTaskClient.execute(workerAddress, fragment);
            }
            System.out.println("DROP TABLE 已执行，元数据与 Worker 本地数据已清理: " + ast.getTableName());
            return null;
        }

        // ==========================================
        // 🌟 阶段 1.5：拦截 INSERT 数据流，全动态哈希路由投递（彻底杜绝硬编码）
        // ==========================================
        if ("INSERT".equals(ast.getStatementType())) {
            // 1. 干掉硬编码，去元数据中心动态拉取目标表的真实 Schema 结构！
            TableMeta tableMeta = metadataManager.getTable(ast.getTableName());
            if (tableMeta == null) {
                throw new RuntimeException("❌ SQL 执行失败: 表不存在 [" + ast.getTableName() + "]");
            }
            
            // 组装真实的列名顺序传给 Worker，让 Worker 知道如何映射 KV
            java.util.List<String> realColumns = new java.util.ArrayList<>();
            // (注意：这里取决于组员 A 在 TableMeta 里写的方法名，通常叫 getColumns)
            for (ColumnMeta col : tableMeta.getColumns()) { 
                realColumns.add(col.getColumnName()); // (或者叫 col.getColumnName())
            }
            ast.setProjectionColumns(realColumns);

            // 动态提取插入数据的分片键（第一列 ID）
            Object idValue = ast.getInsertValues().get(0);
            if (idValue == null) {
                throw new IllegalArgumentException("❌ SQL 执行失败: INSERT 语句的分片键不能为空");
            }

            int partitionId = Math.floorMod(String.valueOf(idValue).hashCode(), 1024);
            if (replicaManager != null) {
                Row row = new Row();
                for (int i = 0; i < realColumns.size(); i++) {
                    row.put(realColumns.get(i), ast.getInsertValues().get(i));
                }
                ReplicaResult writeResult = replicaManager.write(partitionId, ast.getTableName(), row);
                if (!writeResult.isSuccess()) {
                    throw new RuntimeException("❌ SQL 执行失败: " + writeResult.getMessage());
                }
                System.out.println("==> [副本写入] partition=" + partitionId + ", " + writeResult.getMessage());
                return null;
            }

            java.util.List<String> activeWorkers = metadataManager.getActiveWorkers();
            if (activeWorkers == null || activeWorkers.isEmpty()) {
                throw new RuntimeException("❌ SQL 执行失败: 当前分布式计算集群中没有存活的 Worker 节点！");
            }
            int targetIndex = Math.abs(String.valueOf(idValue).hashCode()) % activeWorkers.size();
            String targetWorker = activeWorkers.get(targetIndex);
            TaskFragment fragment = new TaskFragment("insert-" + System.currentTimeMillis(), targetWorker, ast);
            fragmentTaskClient.execute(targetWorker, fragment);
            System.out.println("==> [兼容路径] 成功将数据 [ID=" + idValue + "] 投递至节点: " + targetWorker);
            return null; // 拦截成功，控制流结束
        }

        // ==========================================
        // 🌟 阶段二：数据流下发 (DML/DQL - 查询/操作)
        // 结合元数据进行路由，将子任务 RPC 并发下发给各个 Worker
        // ==========================================
        
        // 2. 生成逻辑计划 (结合元数据做语义校验)
        LogicalPlan logicalPlan = logicalPlanner.build(ast);

        // 3. 生成分布式物理执行计划 (进行算力路由规划)
        DistributedExecutionPlan distributedExecutionPlan = distributedPlanGenerator.generate(logicalPlan);

        // 4. 并发下发 P2P 任务至 Worker 集群
        List<PartialQueryResult> partialResults = new ArrayList<>();
        List<TaskFragment> fragments = distributedExecutionPlan.getFragments();

        // 针对 1 主 2 从（全量副本）架构的随机单节点读（读写分离雏形）
        // 如果 Planner 生成了多个任务（说明是广播查询），且开启了多副本：
        if (fragments.size() > 1 && replicaManager != null) {
            // ⭐ 核心读优化：使用 ThreadLocalRandom 生成随机索引，实现纯随机读负载均衡
            int randomIndex = java.util.concurrent.ThreadLocalRandom.current().nextInt(fragments.size());
            TaskFragment luckyFragment = fragments.get(randomIndex);
            
            System.out.println("==> 负责节点为： " + luckyFragment.getWorkerAddress());
            partialResults.add(fragmentTaskClient.execute(luckyFragment.getWorkerAddress(), luckyFragment));
            
        } else {
            // 如果是带了明确 WHERE id=? 的精准点查（只有一个任务），或者未开启多副本，则保持原逻辑
            for (TaskFragment fragment : fragments) {
                partialResults.add(fragmentTaskClient.execute(fragment.getWorkerAddress(), fragment));
            }
        }

        // 5. 在客户端内存中进行结果聚合 (Reduce)
        return resultMerger.merge(logicalPlan, partialResults);
    }
}
