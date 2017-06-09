package com.alibaba.middleware.race.sync;


import com.alibaba.middleware.race.sync.network.NettyServer;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.play.GlobalComputation;
import com.alibaba.middleware.race.sync.play.SequentialRestore;
import com.alibaba.middleware.race.sync.server.RecordLazyEval;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by will on 6/6/2017.
 */
public class Server {

    static Logger logger;
    static ArrayList<String> dataFiles = new ArrayList<>();
    static NettyServer nserver = null;
    private static SequentialRestore sequentialRestore = new SequentialRestore();

    static {
        for (int i = 1; i < 11; i++) {
            dataFiles.add(i + ".txt");
        }
    }

    String[] args = null;

    public Server(String[] args) {
        this.args = args;
        initProperties();
        logger = LoggerFactory.getLogger(Server.class);
        printArgs(args);
        nserver = new NettyServer(args, Constants.SERVER_PORT);
        nserver.start();
    }

    public static void main(String[] args) {
        RecordLazyEval.schema = args[0];
        RecordLazyEval.table = args[1];
        GlobalComputation.initRange(Long.parseLong(args[2]), Long.parseLong(args[3]));
        try {
            new Server(args).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        for(int i = 10 ; i >0; i--){
            System.out.println(Constants.DATA_HOME + File.separator + dataFiles.get(i-1));
            OneRound(Constants.DATA_HOME + File.separator + dataFiles.get(i-1));
        }

//        for (Map.Entry<Long, String> entry : GlobalComputation.inRangeRecord.entrySet()) {
//            System.out.println(entry.getValue());
//        }
        System.out.println("Send finish all package......");
        nserver.finish();
        nserver.stop();

    }

    private static void OneRound(String fileName) throws IOException {
        long startTime = System.currentTimeMillis();
        ReversedLinesFileReader reversedLinesFileReader = new ReversedLinesFileReader(new File(fileName), 1024 * 1024, Charset.defaultCharset());
        String line;
        String result;
        while ((line = reversedLinesFileReader.readLine()) != null) {
            result = sequentialRestore.compute(line);
            if(result != null){
                logger.info("has result, send to client.....");
                nserver.send(NetworkConstant.LINE_RECORD, result);
            }
        }


        long endTime = System.currentTimeMillis();
        logger.info("computation time:" + (endTime - startTime));
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
