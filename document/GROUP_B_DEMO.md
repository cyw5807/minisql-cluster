# 组员 B 查询引擎联调说明

## 目标

本说明用于演示组员 B 已完成的查询引擎主链路：

- SQL 解析
- 逻辑计划生成
- 基于主键条件的哈希路由 / 广播路由
- RPC 下发 Worker 子任务
- Worker 本地执行过滤、投影、聚合
- Coordinator 汇总最终结果

## 环境前提

本项目已经提供项目内脚本，不会修改系统默认 Java 配置。

需要满足：

- 已安装 `openjdk@17`
- 已安装 `maven`
- 已安装 `zookeeper 3.9.5`

## 启动步骤

### 1. 启动 ZooKeeper

```bash
./scripts/start-zk-local.sh
```

如果输出 `already running`，说明 ZooKeeper 已经启动，可直接继续。

### 2. 启动第一个 Worker

```bash
./scripts/start-worker-demo.sh 9011
```

### 3. 启动第二个 Worker

```bash
./scripts/start-worker-demo.sh 9012
```

### 4. 运行组员 B 的查询演示入口

```bash
./scripts/run-groupb-demo.sh
```

说明：

- `start-worker-demo.sh` 和 `run-groupb-demo.sh` 会自动调用 `prepare-demo.sh`
- `prepare-demo.sh` 只会在项目内执行一次 `mvn install -DskipTests`，用于安装当前多模块构件，不会修改系统默认 Java 配置

## 默认演示 SQL

脚本默认会演示以下三条查询：

```sql
SELECT name FROM student WHERE id = 1001;
SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept;
SELECT dept, AVG(score) AS avg_score FROM student GROUP BY dept;
```

## 自定义 SQL

如需自定义 SQL，可直接运行：

```bash
./scripts/run-groupb-demo.sh "SELECT name FROM student WHERE id = 1003"
```

也可以传多条：

```bash
./scripts/run-groupb-demo.sh   "SELECT name FROM student WHERE id = 1001"   "SELECT dept, COUNT(*) AS cnt FROM student GROUP BY dept"
```

## 测试命令

使用项目内 JDK 17 脚本执行测试：

```bash
./scripts/mvn17.sh test
```

## 当前实现边界

当前优先完成了课程验收最关键的高优先级能力：

- 单表查询
- WHERE 过滤
- 精准路由与广播路由
- COUNT / SUM / AVG / MAX / MIN 聚合
- GROUP BY
- Worker 局部聚合 + Coordinator 最终汇总

当前尚未把分布式 Join 接入到主执行链路中，因此现场演示建议优先展示以上查询闭环。
