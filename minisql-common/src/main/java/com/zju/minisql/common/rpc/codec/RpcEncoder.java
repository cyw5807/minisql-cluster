package com.zju.minisql.common.rpc.codec;

import com.zju.minisql.common.rpc.RpcRequest;
import com.zju.minisql.common.rpc.serialize.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 自定义协议编码器
 * 报文结构：
 * 魔数(2B) | 版本(1B) | 消息类型(1B) | 序列化类型(1B) | RequestID(8B) | Body长度(4B) | Body
 * 总 Header 长度 = 17 Bytes
 */
public class RpcEncoder extends MessageToByteEncoder<Object> {

    private static final short MAGIC_NUMBER = (short) 0xCAFE;
    private static final byte VERSION = 1;
    private final Serializer serializer;

    public RpcEncoder(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        // 1. 判断是请求还是响应，设定 Message Type
        byte messageType = msg instanceof RpcRequest ? (byte) 1 : (byte) 2;
        
        // 2. 获取 Request ID (通过反射或抽象基类获取，此处简化为假设均继承了基础方法)
        long requestId = getRequestId(msg);

        // 3. 序列化 Body
        byte[] bodyBytes = serializer.serialize(msg);
        int bodyLength = bodyBytes.length;

        // 4. 按顺序写入 Header (共 17 字节)
        out.writeShort(MAGIC_NUMBER);      // 2 Bytes
        out.writeByte(VERSION);            // 1 Byte
        out.writeByte(messageType);        // 1 Byte
        out.writeByte((byte) 0);           // 1 Byte (0 代表 Kryo)
        out.writeLong(requestId);          // 8 Bytes
        out.writeInt(bodyLength);          // 4 Bytes

        // 5. 写入 Body
        out.writeBytes(bodyBytes);
    }

    private long getRequestId(Object msg) {
        if (msg instanceof RpcRequest) return ((RpcRequest) msg).getRequestId();
        // 如果是 RpcResponse，自行强转获取，此处省略具体判断
        return 0L; 
    }
}