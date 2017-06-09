package com.alibaba.middleware.race.sync.network.handlers;

import com.alibaba.middleware.race.sync.network.NettyClient;
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

import java.util.Arrays;

/**
 * Created by will on 7/6/2017.
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<String> {
    static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("Channel established......");
        logger.info("Sending a request to get the arguments.....");
        ChannelFuture f = ctx.writeAndFlush(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, ""));
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("Request has sent.....");
            }
        });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        logger.info("Received a message, decoding...");
        char TYPE = msg.charAt(0);
        if (TYPE == NetworkConstant.REQUIRE_ARGS) {
            logger.info("Received a REQUIRE_ARGS reply.....");
            NettyClient.args = new ArgumentsPayloadBuilder(msg.substring(1)).args;
            logger.info(Arrays.toString(NettyClient.args));

        }else
        if(TYPE == NetworkConstant.FINISHED_ALL){
            logger.info("Received all chunks, finished......");
            NettyClient.finishedLock.lock();
            NettyClient.finished = true;
            NettyClient.finishedConditionWait.signalAll();
            NettyClient.finishedLock.unlock();
        }
        
    }

}
