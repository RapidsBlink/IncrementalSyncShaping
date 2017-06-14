package com.alibaba.middleware.race.sync.network.netty;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import com.alibaba.middleware.race.sync.network.netty.handlers.NettyServerHandler;
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

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
    public static ReentrantLock isWriteAbleLock = new ReentrantLock();
    public static Condition isWriteAbleCondition = isWriteAbleLock.newCondition();

    public static StringBuilder sendBuff = new StringBuilder(NetworkConstant.SEND_BUFF_SIZE);
    public static ReentrantLock sendBuffLock = new ReentrantLock();

    static {
        sendBuff.append(NetworkConstant.BUFFERED_RECORD);
    }

    int port;

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    ChannelFuture bindFuture;

    public NettyServer(String[] args, int port) {
        Server.initProperties();
        NettyServer.args = args;

        logger = logger = LoggerFactory.getLogger(NettyServer.class);
        logger.info(Arrays.toString(args));
        this.port = port;
    }

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(2);

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

                }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1 * 1024 * 1024, 100 * 1024 * 1024));

        logger.info("bind to port " + port);

        bindFuture = bootstrap.bind(port);

        logger.info("Bind done.");
    }

    public void finish(){
        if(sendBuff.length() > 1) {
            try {
                sendBuff.append(NetworkConstant.END_OF_TRANSMISSION);
                sendQueue.put(sendBuff.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Server called finish function...");
        //for(int i = 0 ; i < 10; i++)
        send(NetworkConstant.FINISHED_ALL, "");
        logger.info("NettyServer FINISHED_ALL instruction added to the queue...");

        logger.info("wait all message has been sent..");
        while(!sendQueue.isEmpty()){

        }
    }

    public void stop() {
        NettyServer.finished = true;
        while (!NettyServer.sendFinished) {
            //wait here
        }
        try {
            bindFuture.channel().closeFuture().sync();
            workerGroup.shutdownGracefully().sync();
            bossGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("STOP netty server...");

    }

    public void sendWithBuff(char type, String data){
        sendBuffLock.lock();
        if(sendBuff.length() + data.length() > sendBuff.capacity() - 5){
            sendBuff.append(NetworkConstant.END_OF_TRANSMISSION);
            try {
                sendQueue.put(sendBuff.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            sendBuff.setLength(0);
            sendBuff.append(NetworkConstant.BUFFERED_RECORD);
        }
        sendBuff.append(data);
        sendBuff.append(NetworkConstant.END_OF_MESSAGE);
        sendBuffLock.unlock();

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
