package com.zju.minisql.common.rpc;

import com.zju.minisql.common.rpc.client.NettyRpcClient;
import com.zju.minisql.common.rpc.server.NettyRpcServer;
import com.zju.minisql.common.rpc.server.ServiceProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RPC 框架全链路集成测试。
 * 改为动态端口，避免占用用户机器上的固定端口。
 */
public class RpcIntegrationTest {

    private static int testPort;
    private static Thread serverThread;

    @BeforeAll
    public static void startServer() throws InterruptedException, IOException {
        testPort = findFreePort();

        ServiceProvider serviceProvider = new ServiceProvider();
        serviceProvider.registerService(new MockServiceImpl());

        serverThread = new Thread(() -> {
            NettyRpcServer server = new NettyRpcServer(serviceProvider);
            server.start(testPort);
        });
        serverThread.setDaemon(true);
        serverThread.start();

        Thread.sleep(1000);
        System.out.println("====== 测试服务端启动完毕，端口: " + testPort + " ======");
    }

    @Test
    public void testRpcStringCall() {
        NettyRpcClient client = new NettyRpcClient("127.0.0.1", testPort);
        RpcRequest request = new RpcRequest(
                MockService.class.getCanonicalName(),
                "ping",
                new Class<?>[]{String.class},
                new Object[]{"Hello MiniSQL"}
        );
        RpcResponse response = client.sendRequest(request);

        assertTrue(response.isSuccess(), "RPC 调用应该成功，不应抛出异常");
        assertEquals("Pong: Hello MiniSQL", response.getResult(), "返回值应该与 MockServiceImpl 逻辑一致");
    }

    @Test
    public void testRpcMathCall() {
        NettyRpcClient client = new NettyRpcClient("127.0.0.1", testPort);
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

    private static int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
