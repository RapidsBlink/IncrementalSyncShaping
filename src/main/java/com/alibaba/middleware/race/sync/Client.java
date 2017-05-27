package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

    // idle时间
    private final static int readerIdleTimeSeconds = 40;
    private final static int writerIdleTimeSeconds = 50;
    private final static int allIdleTimeSeconds = 100;
    private static String ip;
    private final int port = 5527;
    private EventLoopGroup loop = new NioEventLoopGroup();

    public static void main(String[] args) throws Exception {
        initProperties();
        Logger logger = LoggerFactory.getLogger(Client.class);
        logger.info("Welcome");
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.run();

    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    /**
     * 连接服务端
     * 
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));
                    ch.pipeline().addLast(new ClientIdleEventHandler());
                    ch.pipeline().addLast(new ClientDemoInHandler());
                }
            });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }

    }

    public Bootstrap createBootstrap(Bootstrap bootstrap, EventLoopGroup eventLoop) {

        if (bootstrap != null) {

            bootstrap.group(eventLoop);

            bootstrap.channel(NioSocketChannel.class);

            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {

                @Override

                protected void initChannel(SocketChannel socketChannel) throws Exception {

                    socketChannel.pipeline().addLast(new IdleStateHandler(10, 0, 0));
                    socketChannel.pipeline().addLast(new ClientIdleEventHandler());
                    socketChannel.pipeline().addLast(new ClientDemoInHandler());

                }

            });

            bootstrap.remoteAddress(ip, port);

            bootstrap.connect().addListener(new ConnectionListener(this));

        }

        return bootstrap;

    }

    public void run() {

        createBootstrap(new Bootstrap(), loop);

    }

}
