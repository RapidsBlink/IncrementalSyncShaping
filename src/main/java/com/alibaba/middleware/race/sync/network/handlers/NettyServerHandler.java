package com.alibaba.middleware.race.sync.network.handlers;

import com.alibaba.middleware.race.sync.network.NettyServer;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by will on 7/6/2017.
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<String> {
    static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        //save client channel for further pushing
        if (NettyServer.clientChannel == null || !NettyServer.clientChannel.isActive()) {
            NettyServer.clientChannel = ctx.channel();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        logger.info("RECEIVED a String message......");
        char TYPE = msg.charAt(0);
        if (TYPE == NetworkConstant.REQUIRE_ARGS) {
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

        checkQueueAndSendData();

    }

    private void checkQueueAndSendData() {
        while (!NettyServer.finished) {
            while (!NettyServer.sendQueue.isEmpty()) {
                try {
                    String message = NettyServer.sendQueue.take();
                    if (message.equals("E")) break;
                    logger.info("send a message to client......");
                    NettyServer.clientChannel.writeAndFlush(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.warn("ERROR WHILE TAKE MESSAGE FROM sendQueue......");
                    logger.warn(e.getMessage());
                }
            }
        }
    }


}
