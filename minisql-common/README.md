# MiniSQL Common Module - RPC 框架

本模块是分布式 MiniSQL 系统的核心通信基建，包含了基于 Netty 和 Kryo 的自定义 RPC 框架。

## 模块结构
- `client/`: RPC 客户端实现，负责请求的异步转同步封装。
- `server/`: RPC 服务端实现，负责反射调用本地服务。
- `codec/`: 解决 TCP 粘包拆包的 17 字节协议编解码器。
- `serialize/`: Kryo 高性能序列化工具。

## 自动化回归测试 (Regression Testing)

为了保证底层通信框架的稳定性，我们在 `src/test/java` 目录下编写了基于 JUnit 5 的自动化集成测试 `RpcIntegrationTest`。该测试会启动一个真实的 Netty 端口，并通过 TCP Socket 完成序列化和方法调用的全链路验证。

### 何时需要运行测试？
1. **修改了 `RpcRequest` 或 `RpcResponse` 结构后。**
2. **修改了 `KryoSerializer` 序列化逻辑后。**
3. **修改了 `RpcEncoder` / `RpcDecoder` 的字节头协议长度后。**
4. **每次提交代码到 GitHub (Pull Request) 之前。**

### 如何运行测试？

**方法一：使用命令行 (推荐)**

在 `minisql-cluster` 根目录或 `minisql-common` 目录下，执行以下 Maven 命令：

```bash
mvn clean test -pl minisql-common
```

如果控制台输出 BUILD SUCCESS 且 Failures: 0, Errors: 0，说明底层通信功能正常。

**方法二：使用 IDE (VS Code / IDEA)**

打开 `src/test/java/.../RpcIntegrationTest.java` 文件。

点击类名旁边的 Run Test 或绿色运行箭头。

观察测试控制台面板是否全部通过（绿色）。

### 测试防坑指南

- 如果测试抛出 `BindException: Address already in use`，说明本机的 9999 端口被占用。请修改 `RpcIntegrationTest` 中的 `TEST_PORT` 变量。

- 如果测试抛出序列化异常，请检查是否在 `KryoSerializer` 中正确注册了新增的传输类，或检查对象是否提供了无参构造函数。
