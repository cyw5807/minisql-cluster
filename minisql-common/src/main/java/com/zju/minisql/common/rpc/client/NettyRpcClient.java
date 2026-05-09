package com.zju.minisql.common.rpc.client;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.codec.RpcDecoder;
import com.zju.minisql.common.rpc.codec.RpcEncoder;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * 工业级 Netty RPC 客户端实现 (Smart Client 专用)
 * 特性：全局复用 NIO 线程池、长连接缓存池 (Channel Pooling)、防并发阻塞
 */
public class NettyRpcClient {

    // 1. 全局静态的线程池与启动器，防止线程泄漏
    private static final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private static final Bootstrap bootstrap = new Bootstrap();
    
    // 2. 长连接缓存池：复用与 Worker 之间的 TCP Channel
    private static final Map<String, Channel> channelCache = new ConcurrentHashMap<>();
    
    // 异步回调全局映射表
    private final UnprocessedRequests unprocessedRequests;

    // 静态代码块初始化 Bootstrap，全局只执行一次
    static {
        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true) // 开启 TCP 心跳
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    }

    public NettyRpcClient() {
        this.unprocessedRequests = new UnprocessedRequests();
    }

    /**
     * 核心路由方法：获取或创建安全的 Channel 长连接
     */
    private Channel getChannel(InetSocketAddress inetSocketAddress) {
        String key = inetSocketAddress.toString();
        // 尝试从缓存中获取连接
        if (channelCache.containsKey(key)) {
            Channel channel = channelCache.get(key);
            // 如果连接仍然活跃，直接复用
            if (channel != null && channel.isActive()) {
                return channel;
            } else {
                channelCache.remove(key);
            }
        }

        // 缓存未命中或连接失效，发起新的 TCP 连接
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new RpcDecoder(new KryoSerializer()));
                pipeline.addLast(new RpcEncoder(new KryoSerializer()));
                pipeline.addLast(new NettyClientHandler(unprocessedRequests));
            }
        });

        Channel channel = null;
        try {
            channel = bootstrap.connect(inetSocketAddress).sync().channel();
            channelCache.put(key, channel);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("连接 Worker 节点失败: " + inetSocketAddress, e);
        }
        return channel;
    }

    /**
     * 异步发送 RPC 请求 (底层 P2P 数据流)
     * 遵循 GFS 控制流分离：直接根据目标 IP:Port 投递数据
     */
    public CompletableFuture<RpcResponse> sendRequestAsync(RpcRequest rpcRequest, String targetIp, int targetPort) {
        CompletableFuture<RpcResponse> resultFuture = new CompletableFuture<>();
        InetSocketAddress targetAddress = new InetSocketAddress(targetIp, targetPort);

        // 获取复用的长连接
        Channel channel = getChannel(targetAddress);

        if (!channel.isActive()) {
            throw new IllegalStateException("与 Worker 节点的连接已断开: " + targetAddress);
        }

        // 注册 RequestID 以便接收异步回调
        unprocessedRequests.put(rpcRequest.getRequestId(), resultFuture);

        // 写入网络缓冲区并发包
        channel.writeAndFlush(rpcRequest).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                future.cause().printStackTrace();
                // 发生异常时，清理挂起的 Future
                resultFuture.completeExceptionally(future.cause());
            }
        });

        return resultFuture;
    }

    /**
     * 同步调用的包装方法 (兼容部分老旧业务代码)
     */
    public RpcResponse sendRequestSync(RpcRequest rpcRequest, String targetIp, int targetPort) {
        try {
            return sendRequestAsync(rpcRequest, targetIp, targetPort).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("同步 RPC 调用失败", e);
        }
    }
}