# 组员 B 模块设计与实现说明文档

## 1. 模块定位

组员 B 负责的是分布式 MiniSQL 系统中的**查询引擎与分布式执行模块**。本模块位于系统的计算平面，核心职责不是维护底层数据，也不是维护 ZooKeeper 集群状态，而是完成以下闭环：

```text
SQL 文本
-> 语法解析
-> 逻辑计划构建
-> 分布式路由
-> 子任务下发
-> Worker 本地执行
-> Coordinator 汇总结果
```

从课程作业验收角度看，本模块需要证明系统具备以下能力：

- 能正确理解 SQL 语义
- 能把 SQL 转换成可执行计划
- 能把任务正确路由到目标 Worker
- 能在 Worker 上完成过滤、投影、聚合、Join 等本地计算
- 能在 Coordinator 上完成多 Worker 结果汇总


## 2. 本模块在系统中的位置

本模块依赖组长已经提供的基础设施：

- ZooKeeper 集群协调与服务发现
- 元数据管理 `MetadataManager`
- RPC 通信框架 `NettyRpcClient / NettyRpcServer`
- Worker 注册与 Master 发现 `WorkerRegistry / WorkerDiscovery`

本模块不直接负责：

- 分片数据的真实磁盘存储
- 副本一致性维护
- 主备切换和底层容灾恢复

在当前课程作业实现阶段，为了先打通组员 B 的联调主链路，Worker 侧本地数据先使用**内存表仓库**模拟，后续可以无缝替换为组员 A 的真实存储模块。


## 3. 代码结构总览

### 3.1 查询模型层

位于 `minisql-common/src/main/java/com/zju/minisql/common/query/model/`：

- `QueryAst.java`
  - 保存 SQL 解析后的语义结构
  - 当前支持单表查询、聚合、分组、双表等值 Join
- `FilterCondition.java`
  - 表示 `WHERE` 条件
  - 当前支持 `= != <> > >= < <=`
- `AggregateCall.java` / `AggregateFunction.java`
  - 表示聚合函数调用
  - 当前支持 `COUNT / SUM / AVG / MAX / MIN`
- `Row.java`
  - 统一的行数据表示
  - 同时支持普通列读取和 `table.column` 形式读取
- `TaskFragment.java`
  - Coordinator 下发给单个 Worker 的子任务载体
- `PartialQueryResult.java`
  - Worker 返回给 Coordinator 的局部结果
- `QueryResult.java`
  - 面向客户端的最终结果

### 3.2 Worker 物理执行层

位于 `minisql-common/src/main/java/com/zju/minisql/common/query/executor/`：

- `PhysicalOperator.java`
  - 统一算子接口
- `ScanOperator.java`
  - 本地扫描
- `FilterOperator.java`
  - 本地过滤
- `ProjectOperator.java`
  - 字段裁剪
- `AggregateOperator.java`
  - 本地聚合与局部聚合
- `JoinOperator.java`
  - 本地等值 Join
- `QueryFragmentExecutor.java`
  - Worker 侧子任务执行器
  - 负责将 AST 组装成具体执行链路

### 3.3 Master 查询引擎层

位于 `minisql-master/src/main/java/com/zju/minisql/master/`：

- `parser/`
  - `SqlParser.java`
  - `JSqlParserSqlParser.java`
- `planner/`
  - `LogicalPlan.java`
  - `LogicalPlanner.java`
  - `SimpleLogicalPlanner.java`
  - `DistributedExecutionPlan.java`
  - `DistributedPlanGenerator.java`
  - `SimpleDistributedPlanGenerator.java`
- `router/`
  - `QueryRouter.java`
  - `ExecutionTarget.java`
  - `HashQueryRouter.java`
- `client/`
  - `FragmentTaskClient.java`
  - `RpcFragmentTaskClient.java`
- `merger/`
  - `ResultMerger.java`
- `coordinator/`
  - `DistributedQueryCoordinator.java`
- `demo/`
  - `GroupBQueryDemo.java`

### 3.4 Worker 查询服务层

位于 `minisql-worker/src/main/java/com/zju/minisql/worker/query/`：

- `InMemoryTableRepository.java`
  - 演示阶段的本地内存表数据源
- `DistributedQueryTaskServiceImpl.java`
  - Worker 侧分布式查询服务实现

并通过 `WorkerStarter.java` 注册到现有 RPC 服务框架中。


## 4. 模块启动流程

本模块的启动流程，依赖项目内已经封装好的脚本和现有基础设施。

### 4.1 启动顺序

推荐按照下面的顺序启动：

1. 启动 ZooKeeper
2. 启动一个或多个 Worker
3. 启动组员 B 的 Master 演示入口

### 4.2 启动命令

在项目根目录执行：

```bash
./scripts/start-zk-local.sh
./scripts/start-worker-demo.sh 9011
./scripts/start-worker-demo.sh 9012
./scripts/run-groupb-demo.sh
```

相关脚本说明：

- `scripts/mvn17.sh`
  - 仅对当前命令临时注入 `JDK 17`
  - 不修改系统默认 Java
- `scripts/start-zk-local.sh`
  - 启动本地 ZooKeeper 3.9.5
- `scripts/prepare-demo.sh`
  - 预先执行一次项目内 `mvn install -DskipTests`
  - 解决多模块主类运行时的依赖问题
- `scripts/start-worker-demo.sh`
  - 启动 Worker 演示节点
- `scripts/run-groupb-demo.sh`
  - 启动组员 B 的真实联调入口

### 4.3 启动后的内部流程

#### 第一步：ZooKeeper 启动

通过 `start-zk-local.sh` 启动本地 ZK，供 Worker 注册和 Master 服务发现使用。

#### 第二步：Worker 启动

`start-worker-demo.sh` 最终会运行 `WorkerStarter.java`，内部流程如下：

1. 创建本地 `ServiceProvider`
2. 注册旧的 `SqlExecuteService` 测试服务
3. 注册新的 `DistributedQueryTaskService`
4. 构造本地演示数据仓库 `InMemoryTableRepository`
5. 调用 `WorkerRegistry` 向 ZooKeeper 注册当前节点
6. 启动 `NettyRpcServer` 对外提供 RPC 服务

关键代码文件：

- `minisql-worker/src/main/java/com/zju/minisql/worker/WorkerStarter.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/query/DistributedQueryTaskServiceImpl.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/query/InMemoryTableRepository.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/zk/WorkerRegistry.java`

#### 第三步：Master 侧查询演示入口启动

`run-groupb-demo.sh` 会运行 `GroupBQueryDemo.java`，内部流程如下：

1. 连接 ZooKeeper
2. 启动 `WorkerDiscovery`，获取当前存活 Worker 列表
3. 初始化 `MetadataManager`
4. 若演示表不存在，则自动写入 `student` / `score` 元数据
5. 组装查询协调器 `DistributedQueryCoordinator`
6. 顺序执行默认 SQL 或自定义 SQL
7. 将结果打印到控制台

关键代码文件：

- `minisql-master/src/main/java/com/zju/minisql/master/demo/GroupBQueryDemo.java`
- `minisql-master/src/main/java/com/zju/minisql/master/zk/WorkerDiscovery.java`
- `minisql-master/src/main/java/com/zju/minisql/master/meta/MetadataManager.java`
- `minisql-master/src/main/java/com/zju/minisql/master/coordinator/DistributedQueryCoordinator.java`


## 5. 设计说明

本节重点说明本模块是如何从 SQL 一步步走到分布式执行结果的。

### 5.1 SQL 解析设计

#### 设计目标

把客户端输入的 SQL 字符串解析为系统内部能处理的结构化语义对象，避免后续模块继续操作原始字符串。

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/parser/JSqlParserSqlParser.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/model/QueryAst.java`

#### 当前支持内容

- 单表 `SELECT-FROM-WHERE`
- `GROUP BY`
- `COUNT / SUM / AVG / MAX / MIN`
- 双表等值 Join

#### 设计要点

`JSqlParserSqlParser` 负责：

- 提取主表名
- 提取 Join 表名
- 提取 Join 条件左右列
- 提取投影列
- 提取过滤条件
- 提取分组列
- 提取聚合函数

为了让 Join 查询和普通查询共用后续执行链路，解析阶段会将列名统一为两种形式：

- 普通列：如 `name`
- 限定列：如 `student.name`

这样一来，Join 后投影可以直接按限定列取值。


### 5.2 逻辑计划设计

#### 设计目标

在语法解析完成后，进一步结合表元数据校验 SQL 的合法性，并生成可供后续路由与执行模块使用的逻辑计划。

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/planner/LogicalPlan.java`
- `minisql-master/src/main/java/com/zju/minisql/master/planner/SimpleLogicalPlanner.java`
- `minisql-master/src/main/java/com/zju/minisql/master/metadata/TableMetadataProvider.java`
- `minisql-master/src/main/java/com/zju/minisql/master/metadata/MetadataManagerTableMetadataProvider.java`

#### 设计要点

`SimpleLogicalPlanner` 做了三件关键事情：

1. 读取主表元数据
2. 若存在 Join，则再读取右表元数据
3. 校验 SQL 中涉及的列是否合法

校验内容包括：

- `WHERE` 中的列是否存在
- `SELECT` 中的列是否存在
- `GROUP BY` 中的列是否存在
- 聚合函数中的列是否存在
- Join 条件中的左右列是否存在
- 聚合查询中非聚合列是否都出现在 `GROUP BY` 中

逻辑计划最终保存：

- `QueryAst`
- 主表 `TableMeta`
- Join 表 `TableMeta`

这样后续模块不再需要重复访问元数据。


### 5.3 路由设计

#### 设计目标

根据查询条件决定任务应该发给哪个 Worker。

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/router/HashQueryRouter.java`
- `minisql-master/src/main/java/com/zju/minisql/master/router/ExecutionTarget.java`

#### 当前策略

当前采用简化版哈希路由：

- 如果是主键等值查询，例如 `id = 1001`
  - 直接哈希到单个 Worker
- 如果不满足精准路由条件
  - 广播到所有 Worker
- 如果是 Join 查询
  - 当前直接广播到所有 Worker

#### 为什么 Join 先采用广播

当前演示数据 `student` 和 `score` 是按 `id` 共分片的，因此广播到每个 Worker 后，各个 Worker 都只会在自己的本地分片上做 Join，不会出现重复匹配问题。这样能以较低复杂度先实现“共分片本地 Join”的第一阶段能力。


### 5.4 分布式计划生成设计

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/planner/SimpleDistributedPlanGenerator.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/model/TaskFragment.java`

#### 设计要点

`SimpleDistributedPlanGenerator` 会将逻辑计划拆成多个 `TaskFragment`：

- 每个目标 Worker 一个子任务
- 每个子任务都携带同一份 `QueryAst`
- 目标 Worker 地址保存在 `TaskFragment.workerAddress`

当前策略很适合课程作业阶段：

- 结构清晰
- 易于调试
- 易于和现有 RPC 框架对接


### 5.5 Worker 本地执行设计

#### 实现文件

- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/QueryFragmentExecutor.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/ScanOperator.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/FilterOperator.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/ProjectOperator.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/AggregateOperator.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/executor/JoinOperator.java`

#### 执行策略

`QueryFragmentExecutor` 会根据 `QueryAst` 动态组装执行链路：

##### 单表查询

```text
Scan
-> Filter（如果有 WHERE）
-> Project（如果不是 SELECT *）
```

##### 聚合查询

```text
Scan
-> Filter（如果有 WHERE）
-> Aggregate
```

##### Join 查询

```text
Scan(left)
Scan(right)
-> Join
-> Project
```

#### 设计说明

- `ScanOperator`
  - 负责本地表数据扫描
- `FilterOperator`
  - 负责条件过滤
- `ProjectOperator`
  - 负责投影输出列
- `AggregateOperator`
  - 负责局部聚合
  - 同时支持后续协调节点结果合并
- `JoinOperator`
  - 当前支持双表等值 Join
  - 针对课程演示阶段，采用内存嵌套循环实现

#### Join 的实现策略

为了让 `student.name`、`score.course` 这类列在 Join 后仍然可区分，`QueryFragmentExecutor` 在执行 Join 前会对输入行做限定名包装，例如：

- `student.id`
- `student.name`
- `score.id`
- `score.course`

这样 Join 后的 `Row` 中字段不会混淆，投影和过滤也更稳定。


### 5.6 RPC 下发设计

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/client/RpcFragmentTaskClient.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/service/DistributedQueryTaskService.java`
- `minisql-worker/src/main/java/com/zju/minisql/worker/query/DistributedQueryTaskServiceImpl.java`

#### 设计要点

Coordinator 不直接调用 Worker 的本地方法，而是通过已有 RPC 框架发送请求：

1. `RpcFragmentTaskClient` 将 `TaskFragment` 封装成 `RpcRequest`
2. 通过 `NettyRpcClient` 发往目标 Worker
3. Worker 侧由 `DistributedQueryTaskServiceImpl` 处理请求
4. 执行本地子任务，返回 `PartialQueryResult`

这一层实现了本模块与组长 RPC 框架的真正对接。


### 5.7 结果汇总设计

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/merger/ResultMerger.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/model/PartialQueryResult.java`
- `minisql-common/src/main/java/com/zju/minisql/common/query/model/AggregateState.java`

#### 设计要点

Coordinator 收到多个 Worker 的结果后，分两种情况处理：

##### 非聚合查询

- 直接将多个 Worker 的行结果拼接

##### 聚合查询

- 先按 `GROUP BY` key 建桶
- 再逐个合并各 Worker 的中间聚合状态
- 最终生成真正面向客户端的聚合结果

这种两阶段聚合策略对应设计文档中的要求：

- Worker 先本地聚合
- Coordinator 再最终聚合

这样可以减少网络返回的数据量。


### 5.8 查询协调器设计

#### 实现文件

- `minisql-master/src/main/java/com/zju/minisql/master/coordinator/DistributedQueryCoordinator.java`

#### 核心职责

`DistributedQueryCoordinator` 负责串起整个执行链路：

1. 调用 `SqlParser` 解析 SQL
2. 调用 `LogicalPlanner` 生成逻辑计划
3. 调用 `DistributedPlanGenerator` 生成分布式计划
4. 调用 `FragmentTaskClient` 下发任务
5. 调用 `ResultMerger` 汇总结果

这是整个组员 B 模块的中心调度点。


## 6. 功能详情

### 6.1 已完成功能

当前已经完成以下功能：

- 单表点查
- 单表广播查询
- `WHERE` 条件过滤
- 字段投影
- `GROUP BY`
- `COUNT / SUM / AVG / MAX / MIN`
- Worker 局部聚合
- Coordinator 最终聚合
- 双表等值 Join
- 基于主键等值条件的精准路由
- 基于 RPC 的多 Worker 子任务调度

### 6.2 默认演示 SQL

当前演示入口默认执行以下 SQL：

```sql
SELECT name FROM student WHERE id = 1001;
SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept;
SELECT dept, AVG(score) AS avg_score FROM student GROUP BY dept;
SELECT student.name, score.course FROM student JOIN score ON student.id = score.id;
```

### 6.3 Join 功能当前边界

当前 Join 的实现边界如下：

- 支持双表等值 Join
- 支持 `student.id = score.id` 这种列对列 Join
- 支持 Join 后限定列投影
- 当前不支持多表 Join
- 当前不支持非等值 Join
- 当前不支持 `LEFT JOIN / RIGHT JOIN / OUTER JOIN`
- 当前没有实现重分发 Join


## 7. 测试说明

### 7.1 单元测试

- `minisql-common/src/test/java/com/zju/minisql/common/query/QueryFragmentExecutorTest.java`
  - 测试过滤与投影
  - 测试局部聚合
  - 测试本地 Join

- `minisql-master/src/test/java/com/zju/minisql/master/parser/JSqlParserSqlParserTest.java`
  - 测试聚合 SQL 解析
  - 测试 Join SQL 解析

- `minisql-master/src/test/java/com/zju/minisql/master/router/HashQueryRouterTest.java`
  - 测试精准路由
  - 测试广播路由

### 7.2 集成测试

- `minisql-master/src/test/java/com/zju/minisql/master/coordinator/DistributedQueryCoordinatorIntegrationTest.java`
  - 启动两个真实 RPC Worker
  - 通过 Coordinator 执行点查
  - 通过 Coordinator 执行聚合
  - 通过 Coordinator 执行 Join

### 7.3 全量验证命令

```bash
./scripts/mvn17.sh -q test
```


## 8. 启动与演示建议

如果现场验收需要演示，建议按下面顺序展示：

1. 启动 ZooKeeper
2. 启动两个 Worker，展示它们已注册到 ZK
3. 启动 `GroupBQueryDemo`
4. 依次展示点查、聚合、Join

建议的展示顺序：

- `SELECT name FROM student WHERE id = 1001`
  - 体现精准路由
- `SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept`
  - 体现两阶段聚合
- `SELECT student.name, score.course FROM student JOIN score ON student.id = score.id`
  - 体现本地共分片 Join


## 9. 当前不足与后续可扩展方向

尽管当前已经完成了课程验收最关键的主链路，但仍有进一步扩展空间：

- 将内存表仓库替换为组员 A 的真实存储引擎
- 将路由从简化哈希扩展为真实分片元数据路由
- 支持更复杂的 SQL 条件
- 支持多表 Join
- 支持重分发 Join
- 支持排序和 Limit
- 支持 Join 失败后的重试与故障重路由


## 10. 总结

组员 B 模块当前已经完成从 SQL 到分布式执行结果的完整主链路实现，并且已经与组长提供的 ZooKeeper、元数据管理和 RPC 基础设施完成联调。模块的核心能力可以总结为：

```text
解析 SQL
-> 校验语义
-> 生成计划
-> 路由到 Worker
-> Worker 本地执行
-> Coordinator 汇总结果
```

从课程作业要求来看，当前实现已经覆盖了最关键的验收展示点：

- 单表查询
- 过滤
- 聚合
- 精准路由
- 分布式汇总
- 双表等值 Join

因此本模块已经具备较完整的答辩和现场演示基础。
