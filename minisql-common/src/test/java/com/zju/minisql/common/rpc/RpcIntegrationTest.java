package com.zju.minisql.common.rpc;

import com.zju.minisql.common.rpc.client.NettyRpcClient;
import com.zju.minisql.common.rpc.server.NettyRpcServer;
import com.zju.minisql.common.rpc.server.ServiceProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RPC 框架全链路集成测试
 */
public class RpcIntegrationTest {

    private static final int TEST_PORT = 9999;
    private static Thread serverThread;

    @BeforeAll
    public static void startServer() throws InterruptedException {
        // 1. 初始化服务提供者并注册 Mock 服务
        ServiceProvider serviceProvider = new ServiceProvider();
        serviceProvider.registerService(new MockServiceImpl());

        // 2. 将 Server 放入独立线程运行，防止阻塞测试主线程
        serverThread = new Thread(() -> {
            NettyRpcServer server = new NettyRpcServer(serviceProvider);
            server.start(TEST_PORT);
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // 3. 等待 1 秒钟，确保 Netty 端口绑定成功
        Thread.sleep(1000);
        System.out.println("====== 测试服务端启动完毕 ======");
    }

    @Test
    public void testRpcStringCall() {
        // 1. 创建客户端
        NettyRpcClient client = new NettyRpcClient("127.0.0.1", TEST_PORT);

        // 2. 构造请求对象 (调用 ping 方法)
        RpcRequest request = new RpcRequest(
                MockService.class.getCanonicalName(),
                "ping",
                new Class<?>[]{String.class},
                new Object[]{"Hello MiniSQL"}
        );

        // 3. 发送请求并阻塞等待结果
        RpcResponse response = client.sendRequest(request);

        // 4. 断言结果是否符合预期
        assertTrue(response.isSuccess(), "RPC 调用应该成功，不应抛出异常");
        assertEquals("Pong: Hello MiniSQL", response.getResult(), "返回值应该与 MockServiceImpl 逻辑一致");
    }

    @Test
    public void testRpcMathCall() {
        NettyRpcClient client = new NettyRpcClient("127.0.0.1", TEST_PORT);

        // 构造请求对象 (调用 calculate 方法)
        RpcRequest request = new RpcRequest(
                MockService.class.getCanonicalName(),
                "calculate",
                new Class<?>[]{int.class, int.class},
                new Object[]{10, 20}
        );

        RpcResponse response = client.sendRequest(request);

        assertTrue(response.isSuccess());
        assertEquals(30, response.getResult());
    }

    @AfterAll
    public static void tearDown() {
        if (serverThread != null) {
            serverThread.interrupt();
        }
        System.out.println("====== 测试执行完毕，资源清理 ======");
    }
}