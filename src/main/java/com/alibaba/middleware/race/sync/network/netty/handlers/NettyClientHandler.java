package com.alibaba.middleware.race.sync.network.netty.handlers;

import com.alibaba.middleware.race.sync.client.ClientComputation;
import com.alibaba.middleware.race.sync.network.netty.NettyClient;
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
    static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);

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
        //logger.info("Received a message, decoding...");
        char TYPE = msg.charAt(0);
        if (TYPE == NetworkConstant.REQUIRE_ARGS) {
            logger.info("Received a REQUIRE_ARGS reply.....");
            NettyClient.args = new ArgumentsPayloadBuilder(msg.substring(1)).args;
            logger.info(Arrays.toString(NettyClient.args));

        } else if (TYPE == NetworkConstant.FINISHED_ALL) {
            logger.info("Received all chunks, finished......");
            NettyClient.finishedLock.lock();
            NettyClient.finished = true;
            NettyClient.finishedConditionWait.signalAll();
            NettyClient.finishedLock.unlock();
            ctx.close();
        } else if (TYPE == NetworkConstant.LINE_RECORD) {
            //logger.info("Received a line record.....");
            String data = msg.substring(1);
            long pk = ClientComputation.extractPK(data);
            NettyClient.resultMap.put(pk, data);
            //logger.info(""+NettyClient.resultMap.size());
        } else if(TYPE == NetworkConstant.BUFFERED_RECORD){
            StringBuilder sb = new StringBuilder();
            for(int i = 1 ; i < msg.length(); i++){
              if(msg.charAt(i) != NetworkConstant.END_OF_MESSAGE){
                  sb.append(msg.charAt(i));
              }else{
                  String data = sb.toString();
                  long pk = ClientComputation.extractPK(data);
                  NettyClient.resultMap.put(pk, data);
                  sb.setLength(0);
              }
            }
        } else
            logger.info(TYPE + "decoding error");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("Channel Inactive......");
    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//        logger.info("Channel exceptionCaught......");
//        cause.printStackTrace();
//        ctx.close();
//    }
//
//    @Override
//    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
//        logger.info("Channel channelRegistered......");
//        super.channelRegistered(ctx);
//    }
//
//    @Override
//    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
//        logger.info("Channel channelUnregistered......");
//        super.channelUnregistered(ctx);
//    }
//
//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        logger.info("Channel channelReadComplete......");
//        super.channelReadComplete(ctx);
//    }
//
//    @Override
//    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
//        logger.info("Channel channelWritabilityChanged......");
//        super.channelWritabilityChanged(ctx);
//    }
}
