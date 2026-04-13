package com.zju.minisql.common.rpc.codec;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.RpcResponse;
import com.zju.minisql.common.rpc.serialize.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RpcDecoder extends ByteToMessageDecoder {

    private static final int HEADER_LENGTH = 17;
    private static final short MAGIC_NUMBER = (short) 0xCAFE;
    private final Serializer serializer;

    public RpcDecoder(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 1. 如果可读字节连 Header 都不够，说明是半包，直接等待更多数据
        if (in.readableBytes() < HEADER_LENGTH) {
            return;
        }

        // 2. 标记当前读取位置
        in.markReaderIndex();

        // 3. 读取并校验魔数
        short magic = in.readShort();
        if (magic != MAGIC_NUMBER) {
            // 遇到非法数据，清除缓冲区并关闭连接
            in.clear();
            ctx.close();
            return;
        }

        // 4. 读取 Header 其他字段
        byte version = in.readByte();
        byte messageType = in.readByte();
        byte serializeType = in.readByte();
        long requestId = in.readLong();
        int bodyLength = in.readInt();

        // 5. 校验 Body 长度：如果可读字节小于 Body 长度，说明 Body 还没传完（半包）
        if (in.readableBytes() < bodyLength) {
            // 重置读取位置，假装刚才什么都没读过，等下次数据到来时重试
            in.resetReaderIndex();
            return;
        }

        // 6. 数据完整，读取 Body
        byte[] bodyBytes = new byte[bodyLength];
        in.readBytes(bodyBytes);

        // 7. 根据 Message Type 决定反序列化的目标类
        Class<?> targetClass = messageType == 1 ? RpcRequest.class : RpcResponse.class;
        Object obj = serializer.deserialize(bodyBytes, targetClass);

        // 8. 将解析好的对象传递给下一个 Handler
        out.add(obj);
    }
}