package com.zju.minisql.common.rpc.server;

import com.zju.minisql.common.rpc.codec.RpcDecoder;
import com.zju.minisql.common.rpc.codec.RpcEncoder;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Netty RPC 服务端启动类
 */
public class NettyRpcServer {

    private final ServiceProvider serviceProvider;

    public NettyRpcServer(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    /**
     * 启动服务端监听
     * @param port 监听的端口号
     */
    public void start(int port) {
        // Boss 线程池负责接收新连接，Worker 线程池负责处理读写
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // 开启 Nagle 算法，适用于小数据包
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // 开启 TCP 心跳
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 添加反序列化和序列化器
                            pipeline.addLast(new RpcDecoder(new KryoSerializer()));
                            pipeline.addLast(new RpcEncoder(new KryoSerializer()));
                            // 添加业务处理器
                            pipeline.addLast(new NettyServerHandler(serviceProvider));
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            System.out.println("RPC 服务端已启动，正在监听端口: " + port);
            
            // 阻塞当前线程直到服务端 Channel 关闭
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            System.err.println("RPC 服务端启动被中断");
            Thread.currentThread().interrupt();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}