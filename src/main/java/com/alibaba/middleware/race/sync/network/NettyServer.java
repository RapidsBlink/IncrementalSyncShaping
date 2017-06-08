package com.alibaba.middleware.race.sync.network;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.handlers.NettyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.*;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by will on 7/6/2017.
 */
public class NettyServer {
    static Logger logger;
    public static String[] args;

    int port;
    String dataPath;
    ArrayList<String> fileNames = new ArrayList<>();

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    ChannelFuture bindFuture;

    public NettyServer(String[] args, int port, String dataPath) {
        Server.initProperties();
        NettyServer.args = args;

        logger = logger = LoggerFactory.getLogger("NettyServer");
        logger.info(Arrays.toString(args));
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
                        ch.pipeline().addLast(new SnappyFrameEncoder(), new SnappyFrameDecoder());
                        ch.pipeline().addLast(new DelimiterBasedFrameDecoder(NetworkConstant.MAX_CHUNK_SIZE,
                                Unpooled.wrappedBuffer(NetworkConstant.END_OF_TRANSMISSION.getBytes())));
                        ch.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8), new StringDecoder(CharsetUtil.UTF_8));
                        ch.pipeline().addLast(new NettyServerHandler());

                    }

                }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);

        logger.info("bind to port " + port);

        bindFuture = bootstrap.bind(port);

        logger.info("Bind done.");
    }

    public void stop() {
        bindFuture.channel().closeFuture();
        logger.info("STOP netty server...");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
