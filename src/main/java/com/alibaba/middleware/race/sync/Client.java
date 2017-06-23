package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.network.NativeSocket.NativeClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by will on 6/6/2017.
 */
public class Client {

    public static Logger logger;

    private final static int port = Constants.SERVER_PORT;

    static NativeClient nativeClient = null;

    public static void main(String[] args) {
        new Client(args[0]).start();
        logger.info("Current client time:" + System.currentTimeMillis());
    }

    public Client(String ip) {
        initProperties();
        logger = LoggerFactory.getLogger(Client.class);
//        nettyClient = new NettyClient(ip, Constants.SERVER_PORT);
//        nettyClient.start();
        nativeClient = new NativeClient(ip, Constants.SERVER_PORT);
        nativeClient.start();
    }

    public void start() {
        nativeClient.finish();
        logger.info("before writing:" + System.currentTimeMillis());
        logger.info("" + NativeClient.resultMap.size());
        try {
//            MappedFileWriter bw = new MappedFileWriter(Constants.RESULT_HOME + File.separator + Constants.RESULT_FILE_NAME, 40 * 1024 * 1024);
            BufferedWriter bw = new BufferedWriter(new FileWriter(Constants.RESULT_HOME + File.separator + Constants.RESULT_FILE_NAME));

//            int i = 0;
            for (String value : NativeClient.resultMap.values()) {
//                if (i < 10)
//                    logger.info(value);
                bw.write(value);
//                bw.write(value.getBytes());
                bw.newLine();
//                i++;
            }
            bw.close();

//            File file = new File(Constants.RESULT_HOME + File.separator + Constants.RESULT_FILE_NAME);
//            logger.info("file len:" + file.length());
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
