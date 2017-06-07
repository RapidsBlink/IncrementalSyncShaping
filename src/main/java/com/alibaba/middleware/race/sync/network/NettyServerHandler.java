package com.alibaba.middleware.race.sync.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by will on 7/6/2017.
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<ByteBuf> {
    static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx){
        logger.info("channel active...");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        if(msg.readByte() == NetworkConstant.REQUIRE_FILE){
            logger.info("RECEIVED a REQUIRE_FILE request...");
        }
    }
}
