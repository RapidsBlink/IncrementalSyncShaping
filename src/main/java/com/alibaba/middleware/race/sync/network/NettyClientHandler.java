package com.alibaba.middleware.race.sync.network;

import com.alibaba.middleware.race.sync.network.NettyServer;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by will on 7/6/2017.
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<ByteBuf>{
    static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx){
        logger.info("write a request to server...");
        ByteBuf sendMessage = ctx.alloc().heapBuffer(1);
        ctx.writeAndFlush(sendMessage.writeByte(NetworkConstant.REQUIRE_FILE));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

    }
}
