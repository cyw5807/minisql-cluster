# MiniSQL Master 模块 - 集群控制平面

本模块是分布式 MiniSQL 系统的“大脑”，主要负责整个集群的元数据（Metadata）管理、节点调度以及基于 ZooKeeper 的高可用（HA）容错控制。

## 核心架构与功能
- **Leader 选举**：利用 ZooKeeper 临时节点（Ephemeral Node）机制，确保集群在任何时刻有且仅有一个活跃的 Master。
- **故障感知与转移**：通过 Curator 客户端的 Watcher 机制，实现针对节点意外宕机的秒级接管。

---

## 🚀 高可用 (HA) 容错回归测试手册

本测试用于验证分布式系统中最核心的**“主备切换（Failover）”**逻辑。当你修改了 ZooKeeper 的连接参数、超时时间或协调器逻辑后，必须执行此测试以确保集群的自愈能力没有被破坏。

### 1. 前置环境要求
必须在本地启动 ZooKeeper 服务端，并确保其监听在默认的 `2181` 端口。
- **Windows 启动方式**：在终端运行 `zkServer.cmd`（注意：不需要加 `start` 参数）。
- **Linux/Mac 启动方式**：在终端运行 `zkServer.sh start`。

### 2. 测试入口
测试文件位于本模块的测试目录下：
`src/test/java/com/zju/minisql/master/zk/MasterElectionSimulator.java`

### 3. 测试复现步骤 (Step-by-Step)
1. **启动主节点 (Node A)**：在 IDE 中运行 `MasterElectionSimulator`。
   - *预期结果*：控制台打印 `=== 选举成功！当前节点已成为活跃 Master... ===`
2. **启动备用节点 (Node B)**：**不要关闭 Node A**，再次运行该类启动第二个进程。
   - *预期结果*：控制台打印 `=== 选举失败。当前集群已有活跃 Master... 本节点降级为备用 Master ===`
3. **模拟灾难宕机**：强制终止 Node A 的进程（点击 IDE 控制台的停止按钮或 Kill 终端）。
4. **观察故障转移**：立即切换到 Node B 的控制台视图。
   - *预期结果*：等待约 30 秒（取决于设定的 `sessionTimeoutMs`），Node B 成功感知故障并打印 `!!! 警告: 检测到活跃 Master 宕机... === 选举成功！... ===`

### 4. 常见问题排查 (FAQ)
- **Q: 启动测试时抛出 `ConnectionLossException` 或不停地打印重试日志？**
  - **A**: 本地 ZooKeeper 服务未启动，或者 2181 端口被防火墙拦截。请检查 `zkServer` 是否正常运行。
- **Q: 为什么杀掉 Node A 后，Node B 没有立刻接管，而是等了很久？**
  - **A**: 这是符合预期的。我们在代码中设置了 `sessionTimeoutMs(30000)`，这意味着 ZooKeeper 会给断线的节点 30 秒的“容忍期”来排除网络抖动或断点调试带来的假死。在生产环境部署时，可将其调低至 `5000` 毫秒。
- **Q: Windows 下启动 ZooKeeper 报错 `NumberFormatException`？**
  - **A**: 这是由于 Windows 批处理脚本参数解析机制导致的。请不要使用 `zkServer.cmd start`，直接输入 `zkServer.cmd` 即可。