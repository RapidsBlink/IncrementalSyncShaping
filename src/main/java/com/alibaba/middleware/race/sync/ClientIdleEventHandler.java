package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientIdleEventHandler extends ChannelDuplexHandler {

    Logger logger = LoggerFactory.getLogger(ClientIdleEventHandler.class);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) msg;
            // 维持空闲状态超时间隔作为心跳间隔，server端检查是否要发送批次数据
            if (e.state() == IdleState.READER_IDLE) {
                logger.warn("No message from server, shut down client");
                ctx.close();
            }
        }
    }
}
