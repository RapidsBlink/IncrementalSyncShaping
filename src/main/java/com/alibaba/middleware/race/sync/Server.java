package com.alibaba.middleware.race.sync;


import com.alibaba.middleware.race.sync.NioSocket.NioServer;
import com.alibaba.middleware.race.sync.server2.PipelinedComputation;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.putThingsIntoByteBuffer;

/**
 * Created by will on 6/6/2017.
 */
public class Server {
//    public static Logger logger;
    private static NioServer nativeServer = null;
    private static long start;
    private static long end;

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
        System.setProperty("test.role", Constants.TEST_ROLE[0]);
    }

//    private void printArgs(String[] args) {
//        logger.info(args[0]);
//        logger.info(args[1]);
//        logger.info(args[2]);
//        logger.info(args[3]);
//    }

    public Server(String[] args) {
//        logger.info("Current server time:" + System.currentTimeMillis());
//        printArgs(args);
//        logger.info(Constants.CODE_VERSION);
        start = Long.valueOf(args[2]);
        end = Long.valueOf(args[3]);
    }

    public static void main(String[] args) {

        Server.initProperties();
//        logger = LoggerFactory.getLogger(Server.class);
//        logger.info("Current server time:" + System.currentTimeMillis());

        nativeServer = new NioServer(args, Constants.SERVER_PORT);
        nativeServer.start();


        try {
            new Server(args).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        ArrayList<String> filePathList = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            filePathList.add(Constants.DATA_HOME + File.separator + i + ".txt");
        }
        PipelinedComputation.globalComputation(filePathList, start, end);

        ByteBuffer byteBuffer = ByteBuffer.allocate(40 * 1024 * 1024);
        putThingsIntoByteBuffer(byteBuffer);
        byteBuffer.flip();
        Server.nativeServer.send(byteBuffer);
//        logger.info("second phase end:" + String.valueOf(System.currentTimeMillis()));

        nativeServer.finish();

//        logger.info("size:" + PipelinedComputation.finalResultMap.size());
//        logger.info("Send finish all package......");
    }
}
