# MiniSQL Master Module - 纯粹的集群控制平面 (Control Plane)

本模块是分布式 MiniSQL 系统的"大管家"。在全面升级为 GFS/BigTable 架构后，Master 已彻底剥离了 SQL 解析、执行计划生成与数据流转发等重负载工作，它不再是系统的性能瓶颈。

当前 Master 专注于极其轻量的核心控制逻辑：

- **全局元数据管理 (DDL)**：处理建表、删表等 Schema 变更，并同步至 ZooKeeper。
- **Worker 监控与路由表维护**：监控底层数据平面的存活状态，维护全局路由信息。
- **高可用与 Leader 选举 (HA)**：利用 ZooKeeper 临时节点（Ephemeral Node）机制，确保集群在任何时刻有且仅有一个活跃的 Master 节点进行调度。

## 🚀 高可用 (HA) 容错实战演练手册

本演练用于验证分布式系统中最核心的"主备切换（Failover）"逻辑。当你修改了 ZooKeeper 的连接参数、超时时间或协调器逻辑后，必须执行此测试，以确保集群的自愈能力坚不可摧。

### ⚠️ 测试环境区分说明

请注意，日常的 `mvn test` 单元测试已全部接入 `curator-test` (内存级 ZK)，无需外部依赖。

但本测试属于多进程实战模拟器，因此必须依赖外部真实的 ZooKeeper 环境。

### 1. 前置环境要求

必须在本地启动真实的 ZooKeeper 服务端，并确保其监听在默认的 2181 端口。

- **Windows 启动方式**：在终端运行 `zkServer.cmd`（注意：不需要加 start 参数，直接运行即可）。
- **Linux/Mac 启动方式**：在终端运行 `zkServer.sh start`。

### 2. 测试入口

测试文件位于本模块的测试目录下：

```
src/test/java/com/zju/minisql/master/zk/MasterElectionSimulator.java
```

### 3. 测试复现步骤 (Step-by-Step)

#### 步骤 1：启动主节点 (Node A)

在 IDE 中运行 `MasterElectionSimulator` 的 main 方法。

**预期结果**：控制台打印 `=== 选举成功！当前节点已成为活跃 Master... ===`

#### 步骤 2：启动备用节点 (Node B)

保持 Node A 运行，再次点击运行按钮，启动第二个隔离的模拟器进程。

**预期结果**：控制台打印 `=== 选举失败。当前集群已有活跃 Master... 本节点降级为备用 Master (Standby) ===`

#### 步骤 3：模拟灾难宕机

强制终止 Node A 的进程（点击 IDE 控制台的红色停止按钮或在终端中 Kill 掉）。

#### 步骤 4：观察故障自动转移

立即切换到 Node B 的控制台视图。

**预期结果**：等待约 30 秒（取决于设定的 `sessionTimeoutMs`），Node B 成功感知故障并打印 `!!! 警告: 检测到活跃 Master 宕机... === 选举成功！本节点晋升为活跃 Master ===`

### 4. 常见问题排查 (FAQ)

#### Q: 启动测试时抛出 ConnectionLossException 或不停地打印重试日志？

**A**: 本地物理 ZooKeeper 服务未启动，或者 2181 端口被系统防火墙拦截。请检查 `zkServer` 进程是否正常存活。

#### Q: 为什么杀掉 Node A 后，Node B 没有立刻接管，而是傻等了很久？

**A**: 这是符合预期的心跳机制。我们在代码中设置了 `sessionTimeoutMs(30000)`，这意味着 ZooKeeper 会给断线的节点 30 秒的"容忍期"，以防止网络瞬间抖动或 JVM GC 停顿导致的假死引发频繁切换（脑裂防护）。在生产环境部署时，可根据网络质量将其下调至 5000 毫秒。

#### Q: 为什么 Master 里面找不到分发 Task 的代码了？

**A**: 因为我们已经升级到了 Smart Client 架构！Master 现在是纯粹的控制平面，具体的查询任务已经由客户端（`minisql-client`）直接 P2P 发送给 Worker 了。**不要往这里面塞业务代码！**
