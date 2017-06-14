package com.alibaba.middleware.race.sync.network.netty.handlers;

import com.alibaba.middleware.race.sync.network.netty.NettyServer;
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
    static Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("there comes one connection...");
        NettyServer.clientChannel = ctx.channel();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        logger.info("RECEIVED a String message......");
        char TYPE = msg.charAt(0);
        if (TYPE == NetworkConstant.REQUIRE_ARGS) {
            //save client channel for further pushing
            if (NettyServer.clientChannel == null || !NettyServer.clientChannel.isActive()) {
                NettyServer.clientChannel = ctx.channel();
            }

            logger.info("This is a REQUIRE_ARGS request......");
            ArgumentsPayloadBuilder argsPayload = new ArgumentsPayloadBuilder(NettyServer.args);
            ChannelFuture f = ctx.writeAndFlush(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, argsPayload.toString()));
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("REQUIRE_ARGS reply has sent......");
                }
            });
            checkQueueAndSendData(ctx);
        }
    }

    private void checkQueueAndSendData(final ChannelHandlerContext ctx) {
        while (true) {
            try {
                while(!ctx.channel().isWritable()){
                    logger.info("server flushed..");
                    ctx.flush();
                    Thread.sleep(100);
                }

                final String message = NettyServer.sendQueue.take();
                //logger.info("send a message to client......");
                ChannelFuture f = ctx.write(message);
                if (message.charAt(0) == NetworkConstant.FINISHED_ALL) {
                    NettyServer.clientChannel.flush();
                    logger.info("server try to send FINISHED_ALL package");
                    f.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if(!future.isSuccess()){
                                ctx.writeAndFlush(message).addListener(this);
                            }
                            logger.info("Server send FINISHED_ALL package ...");
                        }
                    });
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.warn("ERROR WHILE TAKE MESSAGE FROM sendQueue......");
                logger.warn(e.getMessage());
            }
        }
    }

//    @Override
//    public void channelInactive(ChannelHandlerContext ctx){
//        logger.info("Channel Inactive......");
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
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
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        logger.info("Channel channelWritabilityChanged......" + ctx.channel().isWritable());
    }

}
