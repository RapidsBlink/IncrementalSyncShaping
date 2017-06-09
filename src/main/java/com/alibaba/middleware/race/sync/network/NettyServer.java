package com.alibaba.middleware.race.sync.network;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import com.alibaba.middleware.race.sync.network.handlers.NettyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
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
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by will on 7/6/2017.
 */
public class NettyServer {
    static Logger logger;
    public static boolean finished = false;
    public static boolean sendFinished = false;
    public static String[] args;
    public static ArrayBlockingQueue<String> sendQueue = new ArrayBlockingQueue<>(NetworkConstant.SEND_CHUNK_BUFF_SIZE);

    public static Channel clientChannel = null;

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
        send(NetworkConstant.FINISHED_ALL, "");
        NettyServer.finished = true;
        while (!NettyServer.sendFinished) {
            //wait here
        }
        bindFuture.channel().closeFuture();
        logger.info("STOP netty server...");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    //send a message, blocking if no space left in send buff
    public void send(char type, String data) {
        try {
            sendQueue.put(NetworkStringMessage.buildMessage(type, data));
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.warn("ERROR WHILE PUTTING DATA INTO QUEUE");
            logger.warn(e.getMessage());
        }

    }
}
