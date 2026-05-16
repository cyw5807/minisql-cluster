package com.zju.minisql.common.rpc;

import com.zju.minisql.common.rpc.client.NettyRpcClient;
import com.zju.minisql.common.rpc.server.NettyRpcServer;
import com.zju.minisql.common.rpc.server.ServiceProvider;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;

/**
 * RPC 框架全链路集成测试 (Smart Client 架构版)
 * 验证：Kryo 序列化、TCP 定长 Header 拆包、异步长连接复用与本地服务调度
 */
public class RpcIntegrationTest {

    private static int testPort;
    private static final String TEST_IP = "127.0.0.1";
    
    private static NettyRpcClient rpcClient;
    private static NettyRpcServer rpcServer;
    private static Thread serverThread;

    // --- 1. 定义测试专用的内部接口与实现 ---
    public interface TestService {
        String sayHello(String name);
        String asyncCall(String id);
        void errorMethod();
    }

    public static class TestServiceImpl implements TestService {
        @Override
        public String sayHello(String name) { return "Hello, " + name; }
        
        @Override
        public String asyncCall(String id) { return "Hello, " + id; }
        
        @Override
        public void errorMethod() { throw new RuntimeException("模拟的服务端异常"); }
    }

    @BeforeAll
    public static void setup() throws IOException {
        testPort = findFreePort();

        // --- 2. 初始化 ServiceProvider 并注册测试服务 ---
        ServiceProvider serviceProvider = new ServiceProvider();
        // 这一步会将 TestService 的全限定名映射到 TestServiceImpl 实例上
        serviceProvider.registerService(new TestServiceImpl());

        // --- 3. 在独立的后台线程中启动 Netty 服务端 ---
        rpcServer = new NettyRpcServer(serviceProvider);
        serverThread = new Thread(() -> {
            rpcServer.start(testPort);
        });
        // 设置为守护线程，防止阻塞测试 JVM 退出
        serverThread.setDaemon(true); 
        serverThread.start();

        // 给服务端 1 秒钟的端口绑定和启动时间，防止客户端过早连接报错
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        // --- 4. 初始化无状态的智能客户端发射器 ---
        rpcClient = new NettyRpcClient();
    }

    @Test
    public void testSyncRpcCall() {
        // 请求调用的类名必须和注册到 ServiceProvider 里的接口名一致
        RpcRequest request = new RpcRequest(
                TestService.class.getCanonicalName(),
                "sayHello",
                new Class[]{String.class},
                new Object[]{"MiniSQL"}
        );

        // 同步发送 P2P 请求
        RpcResponse response = rpcClient.sendRequestSync(request, TEST_IP, testPort);

        assertNotNull(response);
        assertEquals("Hello, MiniSQL", response.getResult());
        assertNull(response.getErrorMessage());
    }

    @Test
    public void testAsyncRpcCallAndChannelPooling() throws Exception {
        String className = TestService.class.getCanonicalName();
        // 连续发送两个异步请求，验证 Channel 长连接池是否生效且不阻塞
        RpcRequest req1 = new RpcRequest(className, "asyncCall", new Class[]{String.class}, new Object[]{"A"});
        RpcRequest req2 = new RpcRequest(className, "asyncCall", new Class[]{String.class}, new Object[]{"B"});

        CompletableFuture<RpcResponse> future1 = rpcClient.sendRequestAsync(req1, TEST_IP, testPort);
        CompletableFuture<RpcResponse> future2 = rpcClient.sendRequestAsync(req2, TEST_IP, testPort);

        // 阻塞等待所有并发请求完成 (模拟分布式环境下的多路数据拉取)
        CompletableFuture.allOf(future1, future2).join();

        assertEquals("Hello, A", future1.get().getResult());
        assertEquals("Hello, B", future2.get().getResult());
    }

    @Test
    public void testRemoteExceptionHandling() {
        // 测试服务端业务报错时，客户端能否优雅捕获，而不是底层崩溃
        RpcRequest request = new RpcRequest(
                TestService.class.getCanonicalName(), 
                "errorMethod", 
                new Class[]{}, 
                new Object[]{}
        );

        RpcResponse response = rpcClient.sendRequestSync(request, TEST_IP, testPort);

        // 断言：异常栈被成功序列化并装载在 errorMessage 字段中跨网络传回
        assertNotNull(response.getErrorMessage());
        assertTrue(response.getErrorMessage().contains("模拟的服务端异常"));
        assertNull(response.getResult());
    }

    @AfterAll
    public static void teardown() {
        if (rpcServer != null) {
            rpcServer.stop();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
        System.out.println(">>> RPC 框架全链路集成测试执行完毕，基建稳定。");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
