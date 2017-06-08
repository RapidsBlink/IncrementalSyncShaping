package com.alibaba.middleware.race.sync.network.handlers;

import com.alibaba.middleware.race.sync.network.TransferClass.NetworkArguments;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by will on 8/6/2017.
 */
public class ArgumentsHandler extends SimpleChannelInboundHandler<NetworkArguments> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NetworkArguments msg) throws Exception {
        
    }
}
