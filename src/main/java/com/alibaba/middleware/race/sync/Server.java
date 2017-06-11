package com.alibaba.middleware.race.sync;


import com.alibaba.middleware.race.sync.network.NettyServer;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.server.ServerPipelinedComputation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static com.alibaba.middleware.race.sync.server.ServerPipelinedComputation.JoinComputationThread;
import static com.alibaba.middleware.race.sync.server.ServerPipelinedComputation.OneRoundComputation;

/**
 * Created by will on 6/6/2017.
 */
public class Server {
    public static Logger logger;
    private static ArrayList<String> dataFiles = new ArrayList<>();
    private static NettyServer nserver = null;

    static {
        for (int i = 1; i < 11; i++) {
            dataFiles.add(i + ".txt");
        }
    }

    private String[] args = null;

    public Server(String[] args) {
        this.args = args;
        initProperties();
        logger = LoggerFactory.getLogger(Server.class);
        printArgs(args);
        logger.info(Constants.CODE_VERSION);
        logger.info("Current server time:" + System.currentTimeMillis());
        nserver = new NettyServer(args, Constants.SERVER_PORT);
        nserver.start();
    }

    public static void main(String[] args) {
        ArrayList<String> reverseOrderFiles = new ArrayList<>();
        for (int i = 10; i > 0; i--) {
            reverseOrderFiles.add(Constants.DATA_HOME + File.separator + dataFiles.get(i - 1));
        }
        try {
            ServerPipelinedComputation.readFilesIntoPageCache(reverseOrderFiles);
        } catch (IOException e) {
            logger.info("preload file failed...");
            logger.info(e.getMessage());
            e.printStackTrace();
        }
        ServerPipelinedComputation.initSchemaTable(args[0], args[1]);
        ServerPipelinedComputation.initRange(Long.parseLong(args[2]), Long.parseLong(args[3]));
        try {
            new Server(args).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        // pipelined computation
        for (int i = 10; i > 0; i--) {
            System.out.println(Constants.DATA_HOME + File.separator + dataFiles.get(i - 1));
            OneRoundComputation(Constants.DATA_HOME + File.separator + dataFiles.get(i - 1), new ServerPipelinedComputation.FindResultListener() {
                @Override
                public void sendToClient(String result) {
                    logger.info("has result, send to client.....");
                    nserver.send(NetworkConstant.LINE_RECORD, result);
                }
            });
        }
        // join computation thread
        JoinComputationThread();

        nserver.finish();
        for (Map.Entry<Long, String> entry : ServerPipelinedComputation.inRangeRecord.entrySet()) {
            logger.info(entry.getValue());
        }
        System.out.println("Send finish all package......");
    }

    void printArgs(String[] args) {
        logger.info(args[0]);
        logger.info(args[1]);
        logger.info(args[2]);
        logger.info(args[3]);
    }

    /**
     * 初始化系统属性
     */
    public static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
        System.setProperty("test.role", Constants.TEST_ROLE[0]);
    }
}
