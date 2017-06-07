package com.alibaba.middleware.race.sync.network;

import com.alibaba.middleware.race.sync.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;

/**
 * Created by will on 7/6/2017.
 */
public class NettyClient {

    EventLoopGroup workGroup = new NioEventLoopGroup(1);
    ChannelFuture sendFuture;

    String ip;
    int port;

    public NettyClient(String ip, int port){
        this.ip = ip;
        this.port = port;
    }

    public void start(){
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workGroup).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new NettyClientHandler());
                        //ch.pipeline().addLast(new Lz4FrameEncoder(), new Lz4FrameDecoder());
                    }
                }).option(ChannelOption.TCP_NODELAY, true);

        sendFuture = bootstrap.connect(ip, port);



    }
    public void close(){
        try {
            sendFuture.channel().closeFuture().sync();
            workGroup.shutdownGracefully();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
