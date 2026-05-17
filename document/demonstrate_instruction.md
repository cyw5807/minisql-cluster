# 🎬 MiniSQL 2.0 分布式集群答辩演示指南

> **战前准备（答辩开始前 5 分钟）**  
> ⚠️ 不要当着老师的面临时开机配置，提前做好以下准备：

## 📋 环境检查清单

- [ ] **清理环境**：确保电脑本地没有运行任何 ZK 进程，占座的黑框全部关掉
- [ ] **启动魔法**：确保代理软件的 TUN 模式（虚拟网卡）已开启
- [ ] **拉起集群**：在终端执行 `docker-compose up -d`，执行 `docker ps` 确保 6 个容器全部是 `Up` 状态
- [ ] **准备多屏展示**：屏幕左边放 IDE（准备运行 CLI），屏幕右边开两个终端窗口，分别准备看主、备 Master 的日志

---

## 🚀 第一幕：秀出集群与服务发现（开场暴击）

### 👨‍💻 操作步骤

1. 在 IDE 中运行你的 `MiniSQLShell`
2. 控制台会打印出酷炫的欢迎语，并停留在 `minisql>` 提示符

### 🎤 答辩话术

> "各位老师好，这就是我们组研发的 **MiniSQL 2.0 分布式版本**。大家现在看到的终端，是我们的 **Smart Client（智能客户端）**。
> 
> 请老师注意，我们**没有在代码里写死任何工作节点的 IP**。客户端启动的瞬间，已经通过底层的 **ZooKeeper 注册中心**，动态拉取到了当前 Docker 容器集群中 **3 个 Worker** 的真实存活地址。接下来，我将在这个原生终端中向集群发送真实的 SQL。"

---

## 🚀 第二幕：展示分布式计算引擎（核心肌肉）

### 👨‍💻 操作步骤

在 `minisql>` 提示符下，一行行输入你准备好的复杂 SQL（可以多行输入，按分号结束）：

### 1️⃣ 结构定义 (DDL) - 创建两张关联表

```sql
CREATE TABLE student (id INT, name VARCHAR(20), dept VARCHAR(20), score INT);
```

```sql
CREATE TABLE score (id INT, course VARCHAR(50));
```

### 2️⃣ 数据灌入 (DML) - 插入分布测试数据

> 💡 提示：你可以把这些语句分别发给不同的 Worker，或者直接在 Client 端连续执行，让 Master 去协调分布

```sql
INSERT INTO student VALUES (1001, 'Bob', 'CS', 95);
INSERT INTO student VALUES (1002, 'David', 'ME', 85);
INSERT INTO student VALUES (1003, 'Alice', 'EE', 92);
INSERT INTO student VALUES (1004, 'Charlie', 'CS', 88);
INSERT INTO student VALUES (1005, 'Eve', 'EE', 90);
```

```sql
INSERT INTO score VALUES (1001, 'ComputerNetwork');
INSERT INTO score VALUES (1002, 'MechanicalDesign');
INSERT INTO score VALUES (1003, 'CircuitAnalysis');
```

### 3️⃣ 终极查询展示 (DQL) - 见证算力

#### ✨ 测试点 1：单表精准点查

```sql
SELECT name FROM student WHERE id = 1001;
```

**预期输出：** `Bob`

---

#### ✨ 测试点 2：分组聚合运算 (GROUP BY & 聚合函数)

```sql
SELECT dept, COUNT(*) AS cnt, AVG(score) AS avg_score 
FROM student 
WHERE score >= 85 
GROUP BY dept;
```

**预期输出：** 展示 CS, ME, EE 三个学院的达标人数和平均分  
> 📌 注意这里考验了 AST 对多算子的解析和 Worker 端的 Reduce 归并

---

#### ✨ 测试点 3：跨表连接大招 (JOIN)

```sql
SELECT student.name, score.course 
FROM student 
JOIN score ON student.id = score.id;
```

**预期输出：**
```
Bob       | ComputerNetwork
David     | MechanicalDesign
Alice     | CircuitAnalysis
```

> 🎯 这证明了你们的分布式 Worker 能够正确处理跨节点的数据匹配！

---

## 🚀 第三幕：HA 高可用与灾难恢复（全场高潮）

### 👨‍💻 操作步骤

#### 步骤 1：展示现有 Master

在右侧第一个终端输入：
```bash
docker logs minisql-master-active
```
让老师看到上面印着 **"已成为活跃 Master"**

#### 步骤 2：展示备用 Master

在右侧第二个终端输入：
```bash
docker logs -f minisql-master-standby
```
让老师看到上面印着 **"降级为备用 Master，监听中..."**

#### 步骤 3：当众"拔网线"

在一个新的终端窗口里，狠狠地敲下强杀命令：
```bash
docker stop minisql-master-active
```

#### 步骤 4：见证奇迹

让老师紧盯第二个（Standby）的终端日志。静静等待大约 **30 秒超时时间**。

随后，屏幕会自动弹出：
```
!!! 警告: 检测到活跃 Master 宕机... 选举成功！本节点晋升为活跃 Master ===
```

### 🎤 答辩话术

> "对于任何一个分布式系统，**单点故障（SPOF）**都是致命的。为了解决这个问题，我们引入了基于 **ZK 临时节点的心跳选举机制**。
> 
> 老师您看，刚才我模拟了主节点机房断电被强杀。在短短几十秒的 Session 容忍期过后，处于待命状态的 **Standby 节点瞬间察觉**，并发起重新选举，**成功接管了集群的控制平面权**！我们的系统，做到了真正的**高可用容灾**！"

---

## 🚀 第四幕：平滑扩缩容（极致加分项，可选）

### 👨‍💻 操作步骤

1. 你的 `MiniSQLShell` 还在运行
2. 回到终端，强行关掉一个 Worker（模拟计算节点宕机）：
   ```bash
   docker stop minisql-worker-9014
   ```
3. 回到 `MiniSQLShell`，再次执行一条简单的 SQL

### 🎤 答辩话术

> "除了主控节点的高可用，我们的计算层也支持**平滑扩缩容**。刚才我强行关闭了一台计算节点（Worker 3）。因为我们做了**动态路由注册**，客户端会立刻将死掉的节点从内存路由表中踢出。
> 
> 大家看，我们再次执行查询，剩下的 **2 个 Worker 依然完美完成了任务**，系统完全没有崩溃，对外服务真正做到了 **0 中断**！"

---

## 👑 答辩总结陈词（Mic Drop）

> "综上所述，区别于传统的单机课设，我们的 **MiniSQL 2.0** 拥有：
> 
> - 🐳 **全容器化部署（Docker Compose）**的物理隔离环境
> - ⚡ 基于 **Netty + ZK** 的高性能 RPC 与动态服务发现机制
> - 🛡️ **主备高可用（HA）** 的集群控制平面
> - 🔥 **真正的分布式算力下推**
> 
> 感谢各位老师的聆听，请老师批评指正！"

---

<div align="center">

**🎉 祝答辩顺利，取得优异成绩！ 🎉**

</div>
