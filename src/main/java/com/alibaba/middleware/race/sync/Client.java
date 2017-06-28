package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.NioSocket.NioClient;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Created by will on 6/6/2017.
 */
public class Client {
//    public static Logger logger;

    private static NioClient nativeClient = null;

    public static void main(String[] args) {
        initProperties();
//        logger = LoggerFactory.getLogger(Client.class);
        new Client(args[0]).start();
//        logger.info("Current client time:" + System.currentTimeMillis());
    }

    public Client(String ip) {
        nativeClient = new NioClient(ip, Constants.SERVER_PORT);
    }

    public void start() {
        try {
            FileChannel fileChannel = new RandomAccessFile(Constants.RESULT_HOME + File.separator + Constants.RESULT_FILE_NAME, "rw").getChannel();
            nativeClient.start(fileChannel);
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
        System.setProperty("test.role", Constants.TEST_ROLE[1]);
    }
}
