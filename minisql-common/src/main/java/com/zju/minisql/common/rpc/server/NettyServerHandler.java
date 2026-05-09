package com.zju.minisql.common.rpc.server;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.reflect.InvocationTargetException;
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
            method.setAccessible(true);
            Object result = method.invoke(service, request.getParameters());
            
            // 3. 封装成功结果
            response = RpcResponse.success(request.getRequestId(), result);
        } catch (Exception e) {
            // 打印完整的服务端异常栈，方便服务端开发人员排错
            e.printStackTrace();
            
            // 【核心修复】：剥去反射包装的塑料布，拿到真正的业务异常
            Throwable realCause = e;
            if (e instanceof InvocationTargetException && e.getCause() != null) {
                realCause = e.getCause();
            }
            
            // 4. 封装失败异常 (跨网络传回真实的错误信息)
            response = RpcResponse.fail(request.getRequestId(), realCause.toString());
        }

        // 5. 将响应写回网络并添加监听器
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