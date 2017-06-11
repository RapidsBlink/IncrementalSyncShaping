package com.alibaba.middleware.race.sync.network;

import com.alibaba.middleware.race.sync.Client;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.network.handlers.NettyClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by will on 7/6/2017.
 */
public class NettyClient {

    public static boolean finished = false;
    public static ReentrantLock finishedLock = new ReentrantLock();
    public static Condition finishedConditionWait = finishedLock.newCondition();
    public static ConcurrentMap<Long, String> resultMap = new ConcurrentSkipListMap<>();

    public static String[] args;
    public static boolean isArgumentsReceived = false;

    EventLoopGroup workGroup = new NioEventLoopGroup(Constants.CLIENT_THREADS_NUMBER);
    ChannelFuture sendFuture;

    String ip;
    int port;

    public NettyClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public void start() {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workGroup).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new SnappyFrameEncoder(), new SnappyFrameDecoder());
                        ch.pipeline().addLast(new DelimiterBasedFrameDecoder(NetworkConstant.MAX_CHUNK_SIZE,
                                Unpooled.wrappedBuffer(NetworkConstant.END_OF_TRANSMISSION.getBytes())));
                        ch.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8), new StringDecoder(CharsetUtil.UTF_8));
                        ch.pipeline().addLast(new NettyClientHandler());
                    }
                }).option(ChannelOption.TCP_NODELAY, true);



        sendFuture = bootstrap.connect(ip, port);
        sendFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()){
                    Client.logger.info("Reconnect......");
                    Thread.sleep(1000,0);
                    sendFuture = bootstrap.connect(ip, port);
                    sendFuture.addListener(this);
                }
            }
        });

    }


    public void waitReceiveFinish(){
        Client.logger.info("Client wait to receive all data.....");
        while(!NettyClient.finished) {
            NettyClient.finishedLock.lock();
            if (!NettyClient.finished) {
                try {
                    NettyClient.finishedConditionWait.await();
                    Client.logger.info("Client awake.....");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Client.logger.info(e.getMessage());
                }
                finally {
                    NettyClient.finishedLock.unlock();
                }
            }else {
                NettyClient.finishedLock.unlock();
            }
        }
        Client.logger.info("Client finished, start to write file.....");
    }

    public void stop() {
        sendFuture.channel().closeFuture();
        try {
            workGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
