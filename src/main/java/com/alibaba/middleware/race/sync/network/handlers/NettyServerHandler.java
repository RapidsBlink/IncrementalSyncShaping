package com.alibaba.middleware.race.sync.network.handlers;

import com.alibaba.middleware.race.sync.network.NettyServer;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by will on 7/6/2017.
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<String> {
    static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        logger.info("RECEIVED a String message......");
        char TYPE = msg.charAt(0);
        if(TYPE == NetworkConstant.REQUIRE_ARGS){
            logger.info("This is a REQUIRE_ARGS request......");
            ArgumentsPayloadBuilder argsPayload = new ArgumentsPayloadBuilder(NettyServer.args);
            ChannelFuture f = ctx.writeAndFlush(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, argsPayload.toString()));
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("REQUIRE_ARGS reply has sent......");
                }
            });
        }
    }


}
