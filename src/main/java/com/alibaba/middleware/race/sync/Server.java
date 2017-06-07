package com.alibaba.middleware.race.sync;


import com.alibaba.middleware.race.sync.play.RecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by will on 6/6/2017.
 */
public class Server {

    static Logger logger;
    static ArrayList<String> dataFiles = new ArrayList<>();

    static {
        for (int i = 1; i < 11; i++) {
            dataFiles.add(i + ".txt");
        }
    }

    String[] args = null;

    public Server(String[] args) {
        this.args = args;
        initProperties();
        logger = LoggerFactory.getLogger("Server");
        printArgs(args);
    }

    public static void main(String[] args) {
        new Server(args).start();
    }

    public void start() {
        for (String fileName : dataFiles) {
            try {
                readFile(fileName);
            } catch (IOException e) {
                e.printStackTrace();
                logger.warn("IOException");
                logger.warn(e.getMessage());
            }
        }

        logger.info("-----------SCHEMA AND TABLE-----------");
        for(String pair : RecordUtil.hashSet){
            logger.info(pair);
        }

    }

    void readFile(String fileName) throws IOException {
        String filteredSchema = args[0];
        String filteredTable = args[1];

        long startTime = System.currentTimeMillis();

        File logFile = new File(Constants.DATA_HOME + File.separator + fileName);

        BufferedReader fileReader = new BufferedReader(new FileReader(logFile));
        ArrayList<String> strList = new ArrayList<>();
        String line;
        int count = 0;
        while ((line = fileReader.readLine()) != null) {
            count++;
            // 1st step: schema, table filter to reduce memory usage
            if (RecordUtil.isSchemaTableOkay(line, filteredSchema, filteredTable)) {
                strList.add(line);
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info("----------" + fileName + "----------");
        logger.info("all line num:" + count);
        logger.info("filtered line num:" + strList.size());
        logger.info("file bytes:" + logFile.length());
        logger.info("average byte per log:" + (float) logFile.length() / count);
        logger.info("read time:" + (endTime - startTime) + " ms");

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
