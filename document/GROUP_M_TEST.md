# 🛡️ MiniSQL 集群测试指南 - GROUP M

## 第一部分：集群协调与元数据测试 (COORD 系列)

---

### 🧪 测试 1：验证 COORD-01 - Master 高可用与防脑裂

**目标：** 证明拔掉主节点网线后，备用节点能接管。

#### 实战步骤

1. **查看当前主节点状态**
   ```bash
   docker logs minisql-master-active
   ```
   > 确认有"成为活跃 Master"之类的日志。

2. **查看备胎状态**
   ```bash
   docker logs minisql-master-standby
   ```
   > 确认处于"监听/备用"状态。

3. **模拟故障（拔网线）**
   ```bash
   docker stop minisql-master-active
   ```

4. **见证切换过程**
   ```bash
   docker logs -f minisql-master-standby
   ```
   盯着日志看。

#### ✅ 验证成功标准

大约 **30 秒**（ZK 会话超时）后，备用节点的日志里弹出：
```
捕获 Node Deleted 事件... 晋升为活跃 Master！
```

---

### 🧪 测试 2：验证 COORD-02 & 03 - Worker 动态扩缩容与异常宕机

**目标：** 证明计算节点热插拔，集群能自动感知。

#### 实战步骤

1. **基线测试**
   
   在 `minisql>` 终端里，正常执行一次查询：
   ```sql
   SELECT * FROM student;
   ```
   确认一切正常。

2. **模拟断电 (COORD-03)**
   
   在另外的终端执行：
   ```bash
   docker stop minisql-worker-9014
   ```
   强杀一个节点。

3. **查看 Master 动态**
   ```bash
   docker logs minisql-master-standby
   ```
   > 因为刚才它上位了，你会看到类似以下日志：
   > ```
   > CHILD_REMOVED... 节点离线，更新路由表
   > ```

4. **模拟扩容 (COORD-02)**
   
   重新启动该节点：
   ```bash
   docker start minisql-worker-9014
   ```

5. **再次查看 Master 日志**
   
   会看到：
   ```
   CHILD_ADDED... 节点上线
   ```

#### ✅ 验证成功标准

在杀掉和重启的过程中，你在 `minisql>` 里执行查询，系统依然能返回结果（只是参与计算的节点变少了），**完美容灾！**

---

### 🧪 测试 3：验证 COORD-04 - DDL 元数据持久化

**目标：** 证明断电重启后，建的表还在。

#### 实战步骤

1. **创建新表**
   
   在 `minisql>` 里建一张新表：
   ```sql
   CREATE TABLE test_persist (id INT);
   ```

2. **毁灭打击：重启控制平面**
   ```bash
   docker restart minisql-master-standby
   ```
   关闭你的 CLI 程序，然后重新运行 `MiniSQLShell`。

3. **验证持久化**
   
   在重新连接的 `minisql>` 里，尝试向新表插数据：
   ```sql
   INSERT INTO test_persist VALUES (1);
   ```

#### ✅ 验证成功标准

- 插入成功
- 没有报"表不存在"的错误
- 证明表结构被永久刻在了 ZK 的 `/minisql/metadata` 路径下，**断电不丢失！**

---

## 第二部分：分布式 RPC 框架测试 (RPC 系列)

---

### 🧪 测试 4：验证 RPC-01 - 高并发与 TCP 粘包防御

**目标：** 证明底层 Netty 扛得住高频请求，且报文不乱码。

#### 实战步骤

> 💡 这个不需要改代码。

在 `minisql>` 终端里，疯狂地复制粘贴一堆极短的 SQL 并按下回车，例如：

```sql
SELECT * FROM student; SELECT * FROM student; SELECT * FROM student; ...
```
（粘贴 20 次同时发出去）

#### ✅ 验证成功标准

- 结果瞬间刷屏，没有卡顿
- **最重要的是**：没有任何一条结果报 `IndexOutOfBoundsException` 或解析乱码
- 证明你们定长的 **16 字节 Header 切包逻辑极其完美**

---

### 🧪 测试 5：验证 RPC-02 - Kryo 序列化与大对象传输

**目标：** 证明复杂的 AST（抽象语法树）能跨网络传输。

#### 实战步骤

执行我们之前准备的那个包含多表、聚合和条件的最复杂语句：

```sql
SELECT student.dept, AVG(student.score) 
FROM student JOIN score ON student.id = score.id 
WHERE score.course = 'ComputerNetwork' 
GROUP BY student.dept;
```

#### ✅ 验证成功标准

这个语句被 JSqlParser 解析后是一个**极其庞大且嵌套极深的 Java 对象**。只要远端 Worker 能算出结果并返回给你，就证明：

- Kryo 序列化不仅把大对象压得很小
- 深拷贝毫无破绽！

---

### 🧪 测试 6：验证 RPC-03 - 异步 Future 超时阻断机制

**目标：** 模拟网络极其卡顿（或假死）时，Client 不会无限期卡死。

#### 实战步骤（利用 Docker 的神仙级操作）

1. **冻结 Worker 容器（模拟假死）**
   
   在你的系统正常运行时，在外面用命令：
   ```bash
   docker pause minisql-worker-9012
   ```

2. **发起查询**
   
   回到 `minisql>`，敲击查询语句：
   ```sql
   SELECT * FROM student;
   ```

3. **观察计时器**
   
   盯着控制台的计时器。

#### ✅ 验证成功标准

因为请求发给 9012 后它被"冻"住了不会回包，客户端在等待达到设定阈值（比如 **3 秒或 5 秒**）后，会精准地抛出 `TimeoutException`，而不是让整个终端永远卡死。

> ⚠️ 测试完记得把它解冻：
> ```bash
> docker unpause minisql-worker-9012
> ```

---

### 🧪 测试 7：验证 RPC-04 - 远端异常的透明回传

**目标：** 证明 Worker 报错时，错误能像本地调用一样显示给用户。

#### 实战步骤

在 `minisql>` 故意执行一句底层肯定会报错的语句，比如：
- 除以零
- 插入类型不匹配的值
- 查询一个不存在的字段

```sql
SELECT unknown_column FROM student;
```

#### ✅ 验证成功标准

终端没有直接崩掉，而是优雅地打印出：
```
ERROR: SQL 执行失败 - 远端 Worker 报错: Column 'unknown_column' not found...
```

证明 Worker 把 `RpcResponse.errorMessage` 完美通过网络传了回来。
