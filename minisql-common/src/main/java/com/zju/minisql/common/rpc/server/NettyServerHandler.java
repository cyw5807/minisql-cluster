package com.zju.minisql.common.rpc.server;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.reflect.Method;

public class NettyServerHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private final ServiceProvider serviceProvider;

    public NettyServerHandler(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) {
        // 1. 根据请求寻找本地的服务实例
        String interfaceName = request.getClassName();
        Object service = serviceProvider.getService(interfaceName);

        RpcResponse response;
        try {
            // 2. 利用反射执行对应的方法
            Method method = service.getClass().getMethod(request.getMethodName(), request.getParameterTypes());
            Object result = method.invoke(service, request.getParameters());
            
            // 3. 封装成功结果
            response = RpcResponse.success(request.getRequestId(), result);
        } catch (Exception e) {
            e.printStackTrace();
            // 3. 封装失败异常
            response = RpcResponse.fail(request.getRequestId(), e);
        }

        // 4. 将响应写回网络并添加监听器
        ctx.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                System.err.println("RPC 响应发送失败: " + future.cause().getMessage());
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("RPC 服务端处理发生异常: " + cause.getMessage());
        ctx.close();
    }
}