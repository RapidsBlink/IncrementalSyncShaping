package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {

    // 保存channel
    private static Map<String, Channel> map = new ConcurrentHashMap<String, Channel>();
    // 接收评测程序的三个参数
    private static String schema;
    private static Map tableNamePkMap;

    public static Map<String, Channel> getMap() {
        return map;
    }

    public static void setMap(Map<String, Channel> map) {
        Server.map = map;
    }

    public static void main(String[] args) throws InterruptedException {
        initProperties();
        printInput(args);
        Logger logger = LoggerFactory.getLogger(Client.class);
        Server server = new Server();
        for (int i = 0; i < 100; i++) {
            logger.info("com.alibaba.middleware.race.sync.Server is running....");
        }

        server.startServer(5527);
    }

    /**
     * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
     * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
     * id>100 and id<200
     */
    private static void printInput(String[] args) {
        // 第一个参数是Schema Name
        System.out.println("Schema:" + args[0]);
        // 第二个参数是Schema Name
        System.out.println("table:" + args[1]);
        // 第三个参数是start pk Id
        System.out.println("start:" + args[2]);
        // 第四个参数是end pk Id
        System.out.println("end:" + args[3]);

    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }


    private void startServer(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // 注册handler
                        ch.pipeline().addLast(new ServerDemoInHandler());
                        // ch.pipeline().addLast(new ServerDemoOutHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
