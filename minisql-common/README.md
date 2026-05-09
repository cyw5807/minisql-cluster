# MiniSQL Common Module - 核心基建与通信底座

本模块是分布式 MiniSQL 系统的基石，包含了基于 Netty 和 Kryo 的自定义 P2P RPC 框架，以及 ZooKeeper 服务发现、集群元数据管理等底层通用组件。它是支撑上层 "Smart Client（智能客户端）" 与 "Worker（数据平面）" 高并发通信的核心。

## 模块结构

- **client/**: 智能客户端专用的 RPC 发射器，支持异步长连接复用与 Future 并发请求。
- **server/**: 纯粹的数据节点 RPC 服务端，内置 ServiceProvider 本地服务注册中心，通过反射自动路由请求并优雅剥离异常（去 InvocationTargetException 包装）。
- **codec/ & serialize/**: 解决 TCP 粘包防抖的定长 Header 协议编解码器，以及 Kryo 高性能序列化引擎。
- **zk/**: ZooKeeper 集群监控组件（WorkerDiscovery），负责节点存活的监听与注册。
- **meta/**: 元数据管理中心，负责分布式环境下的表结构（Schema）存取。

## 自动化回归测试 (Regression Testing)

为了保证底层通信框架和基建的绝对稳定，我们在 `src/test/java` 目录下编写了基于 JUnit 5 的全链路集成测试 `RpcIntegrationTest`。该测试会在后台守护线程中启动真实的 Netty 端口，并验证以下核心能力：

1. Kryo 复杂对象的序列化与 TCP 拆包/粘包处理。
2. CompletableFuture 异步多路复用请求。
3. 服务端反射异常跨网络传回客户端的真实性。

### 何时需要运行测试？

- 修改了 `RpcRequest` 或 `RpcResponse` 的核心字段结构后。
- 修改了 `KryoSerializer` 序列化逻辑，或引入了新的传输实体类后。
- 修改了底层 `pom.xml` 中 Netty 或 Curator (ZooKeeper) 的依赖版本后。
- 调整了 `ServiceProvider` 的路由逻辑后。
- 每次提交代码到主分支 (Pull Request) 之前必须保证全绿。

### 如何运行测试？

#### 方法一：使用命令行 (推荐，模拟 CI/CD 环境)

在 `minisql-cluster` 根目录或 `minisql-common` 目录下，执行以下 Maven 命令：

```bash
mvn clean test -pl minisql-common
```

如果控制台输出 `BUILD SUCCESS` 且 `Failures: 0, Errors: 0`，说明底层通信功能与基建完全正常。

#### 方法二：使用 IDE (VS Code / IDEA)

1. 打开 `src/test/java/.../RpcIntegrationTest.java` 文件。
2. 点击类名旁边的 **Run Test** 或绿色运行箭头。
3. 观察测试控制台面板是否全部通过（绿色）。

## 🚨 测试防坑指南

### 端口被占用

如果测试抛出 `BindException: Address already in use`，说明本机的 9999 端口被其他进程占用。请修改 `RpcIntegrationTest` 中的 `TEST_PORT` 变量号重试。

### 服务未注册

如果客户端收到 `RuntimeException: 找不到对应的服务实现`，请检查服务端是否在启动前正确调用了 `serviceProvider.registerService(...)` 将接口与实现类进行绑定。

### 序列化失败

如果抛出 Kryo 相关的异常，请重点检查传输的实体类是否提供了无参构造函数，或者尝试在 `KryoSerializer` 中显式 register 该类。

### ZooKeeper 丢失

如果涉及到 `meta` 或 `zk` 目录下的测试报出 `ConnectionLossException`，请确保测试类中使用了 `TestingServer`（内存 ZK）或本机已开启 2181 端口的 ZK 服务。
