package com.alibaba.middleware.race.sync.network;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.handlers.NettyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by will on 7/6/2017.
 */
public class NettyServer {
    static Logger logger;
    int port;
    String dataPath;
    ArrayList<String> fileNames = new ArrayList<>();

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    ChannelFuture bindFuture;

    public NettyServer(int port, String dataPath) {
        Server.initProperties();
        logger = logger = LoggerFactory.getLogger("NettyServer");
        this.dataPath = dataPath;
        this.port = port;

        for (int i = 1; i <= 10; i++) {
            fileNames.add(i + ".txt");
        }

    }

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);

        ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(bossGroup, workerGroup).
                channel(NioServerSocketChannel.class).
                childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // 注册handler
                        ch.pipeline().addLast(new NettyServerHandler());
                        ch.pipeline().addLast(new Lz4FrameEncoder(), new Lz4FrameDecoder());
                    }

        }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);

        logger.info("bind to port " + port);

        bindFuture = bootstrap.bind(port);

    }
    public void stop(){
        try {
            logger.info("STOP netty server...");
            bindFuture.channel().closeFuture().sync();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            e.printStackTrace();
        }
    }
}
