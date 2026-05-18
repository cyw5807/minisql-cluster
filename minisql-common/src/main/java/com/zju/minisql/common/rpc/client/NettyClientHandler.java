package com.zju.minisql.common.rpc.client;

import com.zju.minisql.common.rpc.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;

public class NettyClientHandler extends SimpleChannelInboundHandler<RpcResponse> {

    private final UnprocessedRequests unprocessedRequests;

    public NettyClientHandler(UnprocessedRequests unprocessedRequests) {
        this.unprocessedRequests = unprocessedRequests;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) {
        try {
            // 收到响应数据，通知挂起的线程
            unprocessedRequests.complete(msg);
        } finally {
            // 释放 ByteBuf 内存，防止内存泄漏
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (isExpectedDisconnect(cause)) {
            System.err.println("⚠️ RPC 连接已断开，等待自动重连: " + cause.getMessage());
        } else {
            System.err.println("RPC 客户端运行过程发生异常: " + cause.getMessage());
            cause.printStackTrace();
        }
        ctx.close();
    }

    private boolean isExpectedDisconnect(Throwable cause) {
        if (!(cause instanceof IOException)) {
            return false;
        }
        String message = cause.getMessage();
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        return lower.contains("connection reset")
                || lower.contains("forcibly closed")
                || message.contains("中止了一个已建立的连接")
                || message.contains("远程主机强迫关闭了一个现有的连接");
    }
}