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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Netty RPC 客户端实现
 */
public class NettyRpcClient {

    private final String host;
    private final int port;
    private final Bootstrap bootstrap;
    private final UnprocessedRequests unprocessedRequests;

    public NettyRpcClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.unprocessedRequests = new UnprocessedRequests();
        
        EventLoopGroup group = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                // 开启 TCP 底层心跳机制
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // 添加之前写好的编解码器
                        pipeline.addLast(new RpcDecoder(new KryoSerializer()));
                        pipeline.addLast(new RpcEncoder(new KryoSerializer()));
                        // 添加业务处理器
                        pipeline.addLast(new NettyClientHandler(unprocessedRequests));
                    }
                });
    }

    /**
     * 发送 RPC 请求并阻塞等待结果
     */
    public RpcResponse sendRequest(RpcRequest rpcRequest) {
        CompletableFuture<RpcResponse> resultFuture = new CompletableFuture<>();
        try {
            // 1. 连接到目标节点
            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
            Channel channel = channelFuture.channel();

            // 2. 将请求 ID 和对应的 Future 放入 Map
            unprocessedRequests.put(rpcRequest.getRequestId(), resultFuture);

            // 3. 将请求发往网络
            channel.writeAndFlush(rpcRequest).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    future.cause().printStackTrace();
                    resultFuture.completeExceptionally(future.cause());
                }
            });

            // 4. 阻塞等待 NettyClientHandler 接收到响应并调用 complete()
            return resultFuture.get();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("RPC 调用失败", e);
        }
    }
}