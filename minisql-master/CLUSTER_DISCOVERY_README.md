# MiniSQL 分布式集群 - 动态服务发现联调测试指南

本测试用于验证 MiniSQL 集群的核心拓扑感知能力，即：Master 节点能否实时、准确地发现 Worker 节点的上线（动态扩容）与宕机（故障剔除）。

## 1. 核心前置依赖：启动 ZooKeeper 集群大脑
在进行任何分布式测试前，必须确保本地 ZK 注册中心处于运行状态。

* **Windows 环境启动：**
    1. 打开命令行终端 (cmd / PowerShell)。
    2. 切换到 ZooKeeper 的 `bin` 目录（或确保该目录已加入系统环境变量）。
    3. 执行命令：`zkServer.cmd`（注意：切勿添加 start 参数）。
    4. 若控制台输出 `binding to port 0.0.0.0/0.0.0.0:2181` 且光标持续闪烁，说明 ZK 已就绪。**请勿关闭此窗口。**

## 2. 联调测试步骤 (Step-by-Step)

### 步骤一：启动 Master 监听雷达
* **目标文件：** `minisql-master/src/test/java/com/zju/minisql/master/zk/ClusterDiscoverySimulator.java`
* **操作：** 运行该类的 `main` 方法。
* **预期现象：** 控制台输出 `[提示] Master 正在持续监听 Worker 的上下线事件...`，此时 Master 开始在后台轮询 `/minisql/workers` 节点。

### 步骤二：节点接入 (Worker 1 上线)
* **目标文件：** `minisql-worker/src/main/java/com/zju/minisql/worker/WorkerStarter.java`
* **操作：** 默认端口为 `9011`，直接运行该类的 `main` 方法。
* **预期现象：** * Worker 端输出：`本地 Worker 已成功注册到 ZK 集群`。
    * **Master 端输出：** `✨ [动态扩容] 新 Worker 上线: 127.0.0.1:9011，当前可用列表: [127.0.0.1:9011]`

### 步骤三：动态扩容 (Worker 2 上线)
* **目标文件：** 同上 (`WorkerStarter.java`)
* **操作：** 修改代码 `int port = args.length > 0 ? Integer.parseInt(args[0]) : 9012;`（或者通过启动参数传入 `9012`），再次运行该方法启动第二个 Worker 进程。
* **预期现象：**
    * **Master 端输出：** `✨ [动态扩容] 新 Worker 上线: 127.0.0.1:9012，当前可用列表: [127.0.0.1:9011, 127.0.0.1:9012]`

### 步骤四：故障模拟与剔除 (Failover 触发)
* **操作：** 在 IDE 的进程管理面板或终端中，**强制终止** 一号 Worker (端口 9011) 的进程。
* **预期现象：**
    * 等待约 5 秒钟（即 ZK 会话超时时间）。
    * **Master 端输出：** `⚠️ [故障感知] Worker 下线: 127.0.0.1:9011，当前可用列表: [127.0.0.1:9012]`

## 3. 常见排错指南 (Troubleshooting)
* **Q: Worker 启动报 `java.net.BindException: Address already in use`？**
    * **A:** 上一个测试进程未彻底关闭，导致端口被占用。使用 `netstat -ano | findstr :901X` 查出 PID，并用 `taskkill /PID <PID> /F` 强制结束僵尸进程。
* **Q: Master 日志输出存在大段的 SLF4J 警告？**
    * **A:** 这是日志框架未找到具体实现（如 Logback）的默认行为，不影响底层 RPC 和 ZK 逻辑，属于正常现象。