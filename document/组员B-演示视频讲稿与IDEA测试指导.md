# 组员 B 演示视频讲稿与 IDEA 测试指导

## 1. 视频目标与时间安排

演示视频要求 3-5 分钟，建议控制在 4 分钟左右，结构如下：

| 环节 | 建议时长 | 内容 |
|---|---:|---|
| PPT 讲解 | 2 分 30 秒 | 介绍模块定位、架构、关键实现、测试覆盖 |
| IDEA 启动项目 | 1 分钟 | 启动 ZooKeeper、两个 Worker、Smart Client 演示入口 |
| 测试结果展示 | 30-60 秒 | 展示查询输出和自动化测试通过结果 |

建议录制时不要逐页详细展开 13 页 PPT，而是按主题快速讲解。PPT 中每页底部已有“讲述提示”，录制时可结合本文讲稿。


## 2. PPT 讲稿

### 第 1 页：标题页

大家好，我负责的是组员 B 的分布式查询引擎模块。这个模块的核心目标是把用户输入的 SQL 查询，转换成可以在多个 Worker 节点上执行的分布式任务，最终再把各个节点的局部结果汇总成完整查询结果。

可以概括为一条主链路：SQL 解析、逻辑计划、任务路由、本地执行、结果汇总。


### 第 2 页：模块目标与职责边界

本模块主要负责查询计算层，不直接负责底层磁盘存储、副本一致性和主备选举。

具体来说，我们实现了 SQL 解析、逻辑计划生成、基于主键的查询路由、Worker 本地执行算子，以及 Coordinator 结果汇总。

当前已经支持单表查询、WHERE 过滤、字段投影、GROUP BY 聚合、COUNT/SUM/AVG/MAX/MIN 五类聚合函数，以及双表等值 Join。


### 第 3 页：系统位置与模块交互

从系统整体看，组员 B 模块位于 Smart Client 或查询协调器的位置。

它会通过 ZooKeeper 发现当前存活的 Worker，通过 MetadataManager 读取表结构，通过 DistributionManager 获取分片路由信息，最后通过 RPC 把查询子任务发送给 Worker。

Worker 侧执行完本地子任务后，会返回局部结果，查询协调器再进行统一汇总。


### 第 4 页：端到端查询执行链路

这一页是本模块的核心流程。

用户提交 SQL 后，`JSqlParserSqlParser` 先把 SQL 解析成 `QueryAst`。然后 `SimpleLogicalPlanner` 结合表元数据生成 `LogicalPlan` 并校验字段是否合法。

接着，`HashQueryRouter` 根据查询条件选择目标 Worker。如果是主键等值查询，就可以精准路由到一个 Worker；否则广播到多个 Worker。

随后 `SimpleDistributedPlanGenerator` 生成多个 `TaskFragment`，再由 `RpcFragmentTaskClient` 通过 RPC 下发。Worker 执行完成后，`ResultMerger` 负责合并结果，最终生成 `QueryResult`。


### 第 5 页：核心数据结构设计

为了让各个阶段解耦，我们设计了几个核心数据结构。

`QueryAst` 表示 SQL 解析后的语义对象，里面包含表名、投影列、过滤条件、分组列、聚合函数和 Join 信息。

`TaskFragment` 是 Coordinator 下发给 Worker 的子任务，里面携带目标 Worker 地址、分区编号和同一份查询语义。

`PartialQueryResult` 是 Worker 返回的局部结果，既可以保存普通行结果，也可以保存聚合中间状态。

`AggregateState` 用来支持两阶段聚合，Worker 先局部聚合，Coordinator 再最终合并。


### 第 6 页：SQL 解析与语义校验

SQL 解析器使用 JSqlParser 实现，主要代码在 `JSqlParserSqlParser.java`。

它支持解析 SELECT、WHERE、GROUP BY、聚合函数以及双表等值 Join。

解析完成后，逻辑计划生成器 `SimpleLogicalPlanner.java` 会读取表元数据，校验投影列、过滤列、分组列、聚合列和 Join 条件是否合法。

例如聚合查询中，非聚合字段必须出现在 GROUP BY 中；Join 查询中，左右两张表和 Join 条件字段都必须存在。


### 第 7 页：查询路由与分布式计划

路由模块主要由 `HashQueryRouter.java` 实现。

如果查询条件是主键等值查询，比如 `WHERE id = 1001`，就可以根据主键哈希定位到单个 Worker，减少网络开销。

如果查询不包含分片键，或者是 Join 查询，就广播到所有 Worker，让每个 Worker 在自己的本地分片上执行。

生成的路由目标会被转换成多个 `TaskFragment`，每个 Worker 一个子任务。


### 第 8 页：Worker 本地物理执行

Worker 侧由 `QueryFragmentExecutor.java` 负责本地执行。

它会根据 `QueryAst` 组装不同的算子链。

普通查询是 `Scan -> Filter -> Project`；聚合查询是 `Scan -> Filter -> Aggregate`；Join 查询是左右两张表分别扫描后执行 `Join -> Project`。

物理算子都实现统一的 `PhysicalOperator` 接口，包含 `open / next / close` 三个生命周期方法。


### 第 9 页：结果汇总与两阶段聚合

结果汇总由 `ResultMerger.java` 实现。

对于普通查询和 Join 查询，Coordinator 会直接拼接多个 Worker 返回的行结果。

对于聚合查询，Worker 不返回所有原始行，而是返回 `AggregateBucket` 和 `AggregateState`。Coordinator 按 GROUP BY Key 合并这些状态，最终计算出完整结果。

这样可以减少网络传输量，也更接近真实分布式数据库中的两阶段聚合。


### 第 10 页：Join 实现策略

当前 Join 采用共分片本地 Join 的第一阶段实现。

演示数据中 `student` 表和 `score` 表都按 `id` 分片，因此相同 `id` 的两张表记录会落在同一个 Worker 上。

Join 查询会广播给所有 Worker，每个 Worker 只对自己的本地分片执行等值 Join，最后 Coordinator 拼接所有 Worker 的 Join 结果。

为了避免字段冲突，Join 前会把字段包装成 `student.name`、`score.course` 这种限定列名。


### 第 11 页：启动流程与演示方式

实际演示时，我们先启动 ZooKeeper，再启动两个 Worker，最后运行 Smart Client 演示入口。

两个 Worker 会注册到 ZooKeeper，并加载本地演示数据。Smart Client 会通过 WorkerDiscovery 发现节点，初始化元数据，然后执行默认 SQL。

默认 SQL 覆盖了点查、聚合、平均值计算和 Join 查询。


### 第 12 页：测试设计与验证结果

测试方面，我们覆盖了四类测试。

第一类是解析测试，验证聚合 SQL 和 Join SQL 能正确解析。

第二类是 Worker 本地执行测试，验证过滤、投影、局部聚合和本地 Join。

第三类是路由测试，验证主键等值查询走精准路由，非分片键查询走广播路由。

第四类是端到端集成测试，会启动两个 RPC Worker，执行点查、聚合和 Join，验证完整分布式链路。

最新执行 `./scripts/mvn17.sh -q test` 已经全量通过。


### 第 13 页：总结与后续扩展

总结一下，组员 B 模块已经完成从 SQL 到分布式执行结果的完整闭环。

它能解析 SQL，生成计划，路由到 Worker，通过 RPC 下发任务，在 Worker 上本地执行，并在 Coordinator 上完成结果汇总。

后续如果继续扩展，可以接入真实存储引擎，支持更复杂的 WHERE 条件、ORDER BY、LIMIT，以及重分发 Join 和更多 Join 类型。


## 3. IDEA 中启动项目进行演示

### 3.1 准备工作

打开 IDEA 后，建议先确认以下配置：

1. 项目根目录为 `minisql-cluster`。
2. Project SDK 使用 JDK 17。
3. Maven 已识别根目录 `pom.xml`。
4. 本地 ZooKeeper 版本为 Apache ZooKeeper 3.9.5。
5. 如果你电脑上还有 Java 8 项目，不要改系统全局 `JAVA_HOME`，只在 IDEA 当前项目或终端命令里使用 JDK 17。

如果 IDEA 里 Maven 依赖还没加载完整，可以先在 IDEA Terminal 里执行：

```bash
./scripts/mvn17.sh -q -DskipTests install
```

预期结果：命令正常结束，没有 `BUILD FAILURE`。


### 3.2 启动 ZooKeeper

在 IDEA 底部 Terminal 中执行：

```bash
./scripts/start-zk-local.sh
```

预期结果：

```text
STARTED
```

如果看到类似 `already running`，也表示 ZooKeeper 已经在运行，可以继续下一步。

涉及文件：

- `scripts/start-zk-local.sh`


### 3.3 启动 Worker 1

在 IDEA 中创建一个 Run Configuration：

| 配置项 | 内容 |
|---|---|
| 类型 | Application |
| Name | `Worker-9011` |
| Main class | `com.zju.minisql.worker.WorkerStarter` |
| Module | `minisql-worker` |
| Program arguments | `9011` |
| JRE | JDK 17 |
| Working directory | 项目根目录 `minisql-cluster` |

点击运行。

预期输出包含：

```text
成功注册服务: com.zju.minisql.common.service.SqlExecuteService
成功注册服务: com.zju.minisql.common.query.service.DistributedQueryTaskService
本地服务初始化完成。
已加载演示数据，并注册分布式查询执行服务。
Worker 已注册到 ZooKeeper: 127.0.0.1:9011
RPC 服务端已启动，正在监听端口: 9011
```

涉及代码：

- `minisql-worker/src/main/java/com/zju/minisql/worker/WorkerStarter.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/query/DistributedQueryTaskServiceImpl.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/query/InMemoryTableRepository.java`


### 3.4 启动 Worker 2

复制上一个 Run Configuration，改成：

| 配置项 | 内容 |
|---|---|
| Name | `Worker-9012` |
| Main class | `com.zju.minisql.worker.WorkerStarter` |
| Module | `minisql-worker` |
| Program arguments | `9012` |
| JRE | JDK 17 |
| Working directory | 项目根目录 `minisql-cluster` |

点击运行。

预期输出包含：

```text
Worker 已注册到 ZooKeeper: 127.0.0.1:9012
RPC 服务端已启动，正在监听端口: 9012
```

注意：两个 Worker 都是长运行进程，录制期间保持运行，不要停止。


### 3.5 启动组员 B 查询演示入口

再创建一个 Run Configuration：

| 配置项 | 内容 |
|---|---|
| 类型 | Application |
| Name | `GroupBQueryDemo` |
| Main class | `com.zju.minisql.client.demo.GroupBQueryDemo` |
| Module | `minisql-client` |
| Program arguments | 可为空 |
| JRE | JDK 17 |
| Working directory | 项目根目录 `minisql-cluster` |

点击运行。

预期输出首先会显示模块联调信息：

```text
[A-模块] 数据分布路由 key=1001 -> 127.0.0.1:xxxx
[A-模块] 副本写入结果: ...
[A-模块] 副本读取路由: ...
========================================
组员 B 查询引擎联调演示开始 (Smart Client 节点)
ZooKeeper 地址: 127.0.0.1:2181
当前 Worker 列表: [...]
========================================
```

随后会执行 4 条默认 SQL。


## 4. 演示 SQL 与预期结果

### 4.1 点查 SQL

执行 SQL：

```sql
SELECT name FROM student WHERE id = 1001;
```

预期结果：

```text
name
Alice
```

说明：该查询命中主键等值条件，路由器可以进行精准路由。

关键代码：

- `minisql-client/src/main/java/com/zju/minisql/client/router/HashQueryRouter.java`
- `minisql-client/src/main/java/com/zju/minisql/client/coordinator/DistributedQueryCoordinator.java`


### 4.2 分组聚合 SQL

执行 SQL：

```sql
SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept;
```

预期结果：

```text
dept | cnt
CS   | 2
EE   | 1
```

说明：每个 Worker 先对本地分片做局部聚合，Coordinator 再合并多个 Worker 的 `AggregateState`。

关键代码：

- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/AggregateOperator.java`
- `minisql-client/src/main/java/com/zju/minisql/client/merger/ResultMerger.java`


### 4.3 平均值聚合 SQL

执行 SQL：

```sql
SELECT dept, AVG(score) AS avg_score FROM student GROUP BY dept;
```

预期结果：

```text
dept | avg_score
CS   | 93.0
EE   | 92.5
ME   | 84.0
```

说明：AVG 需要维护 sum 和 count 两类中间信息，最终由 `AggregateState.finalValue()` 计算平均值。

关键代码：

- `minisql-common/src/main/java/com/zju/minisql/common/query/model/AggregateState.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/model/AggregateFunction.java`


### 4.4 Join SQL

执行 SQL：

```sql
SELECT student.name, score.course FROM student JOIN score ON student.id = score.id;
```

预期结果包含 5 行：

```text
student.name | score.course
Alice        | DistributedDB
Bob          | ComputerNetwork
Carol        | OperatingSystem
David        | MechanicalDesign
Eve          | PowerSystem
```

说明：`student` 和 `score` 两张演示表都按 `id` 共分片，因此每个 Worker 可以在本地完成等值 Join，Coordinator 最后拼接所有 Worker 的 Join 结果。

关键代码：

- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/JoinOperator.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/QueryFragmentExecutor.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/query/InMemoryTableRepository.java`


## 5. IDEA 中运行自动化测试

### 5.1 推荐方式一：运行全量测试

在 IDEA Terminal 中执行：

```bash
./scripts/mvn17.sh -q test
```

预期结果：

```text
命令退出码为 0
没有 BUILD FAILURE
```

其中 RPC 测试可能会打印一段“模拟的服务端异常”堆栈，这是测试 `errorMethod()` 的预期行为，不代表测试失败。真正判断标准是 Maven 命令退出码为 0。


### 5.2 推荐方式二：运行组员 B 端到端测试

在 IDEA 中打开测试文件：

```text
minisql-client/src/test/java/com/zju/minisql/client/coordinator/DistributedQueryCoordinatorIntegrationTest.java
```

点击类名或 `testCoordinatorExecute()` 左侧的运行按钮。

该测试会自动：

1. 获取两个动态空闲端口。
2. 启动两个本地 Netty RPC Worker。
3. 构造分片后的 `student` 和 `score` 数据。
4. 创建 `DistributedQueryCoordinator`。
5. 执行点查、聚合和 Join。
6. 使用断言验证结果。

核心断言包括：

```java
assertEquals("Alice", pointQueryResult.getRows().get(0).get("name"));
assertEquals(2L, countByDept.get("CS"));
assertEquals(1L, countByDept.get("EE"));
assertEquals(5, joinResult.getRows().size());
assertEquals("DistributedDB", joinResult.getRows().get(0).get("score.course"));
```

预期结果：测试绿色通过。


## 6. 录屏时建议展示的代码文件

如果视频中需要快速切换代码，可以按下面顺序打开：

| 展示顺序 | 文件 | 说明 |
|---:|---|---|
| 1 | `minisql-client/src/main/java/com/zju/minisql/client/coordinator/DistributedQueryCoordinator.java` | 查询主链路：解析、计划、下发、汇总 |
| 2 | `minisql-client/src/main/java/com/zju/minisql/client/parser/JSqlParserSqlParser.java` | SQL 解析 |
| 3 | `minisql-client/src/main/java/com/zju/minisql/client/planner/SimpleLogicalPlanner.java` | 元数据校验与逻辑计划 |
| 4 | `minisql-client/src/main/java/com/zju/minisql/client/router/HashQueryRouter.java` | 精准路由与广播路由 |
| 5 | `minisql-common/src/main/java/com/zju/minisql/common/query/executor/QueryFragmentExecutor.java` | Worker 本地执行链路 |
| 6 | `minisql-client/src/main/java/com/zju/minisql/client/merger/ResultMerger.java` | 普通结果与聚合结果汇总 |
| 7 | `minisql-client/src/test/java/com/zju/minisql/client/coordinator/DistributedQueryCoordinatorIntegrationTest.java` | 端到端测试 |

录制 3-5 分钟时，不建议展开太多代码。最推荐展示 `DistributedQueryCoordinator.java` 和 `DistributedQueryCoordinatorIntegrationTest.java`，一个说明主流程，一个证明测试闭环。


## 7. 常见问题与处理

### 7.1 端口被占用

如果 Worker 启动时报端口占用，例如 `9011` 或 `9012` 被占用，可以在 IDEA Terminal 中查看：

```bash
lsof -nP -iTCP:9011 -sTCP:LISTEN
lsof -nP -iTCP:9012 -sTCP:LISTEN
```

如果确认是之前录制时启动的 Worker，可以停止对应 IDEA Run 窗口，或者执行：

```bash
kill <PID>
```


### 7.2 ZooKeeper 已经运行

如果启动 ZooKeeper 时提示 already running，可以直接继续启动 Worker。

如果需要停止 ZooKeeper，可以执行：

```bash
./scripts/stop-zk-local.sh
```


### 7.3 IDEA 找不到类或依赖

先执行：

```bash
./scripts/mvn17.sh -q -DskipTests install
```

然后在 IDEA Maven 面板中点击 Reload All Maven Projects。


### 7.4 全量测试中出现“模拟的服务端异常”

这是 `RpcIntegrationTest` 专门测试远程异常传递能力时主动抛出的异常。

只要 Maven 最终退出码为 0，或者 IDEA 显示测试绿色通过，就说明测试成功。


## 8. 录制时的推荐操作顺序

1. 先打开 PPT，全屏讲解第 1-13 页，控制在 2 分 30 秒左右。
2. 切换到 IDEA，展示 `DistributedQueryCoordinator.java`，说明主链路。
3. 打开 Terminal，执行或展示 ZooKeeper 已启动。
4. 运行 `Worker-9011` 和 `Worker-9012` 两个 Run Configuration。
5. 运行 `GroupBQueryDemo`。
6. 展示 4 条 SQL 的输出结果，重点指出点查、聚合、Join。
7. 最后运行或展示 `DistributedQueryCoordinatorIntegrationTest` 绿色通过。
8. 总结：组员 B 模块已经完成 SQL 解析、计划生成、分布式路由、Worker 执行和结果汇总的完整闭环。

