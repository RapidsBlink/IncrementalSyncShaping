package com.alibaba.middleware.race.sync;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;

/**
 * Created by wanshao on 2017/5/26.
 */
public class ConnectionListener implements ChannelFutureListener {

    Logger logger = LoggerFactory.getLogger(ConnectionListener.class);
    private Client client;

    public ConnectionListener(Client client){

        this.client = client;

    }

    @Override

    public void operationComplete(ChannelFuture channelFuture) throws Exception {

        if (!channelFuture.isSuccess()) {

            logger.info("Try to reconnect because remote server have no response....");
            final EventLoop loop = channelFuture.channel().eventLoop();

            loop.schedule(new Runnable() {

                @Override

                public void run() {

                    client.createBootstrap(new Bootstrap(), loop);

                }

            }, 1L, TimeUnit.SECONDS);

        }

    }
}
