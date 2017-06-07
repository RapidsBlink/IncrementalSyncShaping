package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by will on 6/6/2017.
 */
public class Client {

    static Logger logger;

    private final static int port = Constants.SERVER_PORT;


    public static void main(String[] args){
        new Client().start();
    }

    public Client(){
        initProperties();
        logger = LoggerFactory.getLogger("Client");

    }

    public void start(){

        while(true) {
            logger.info("Client goes to sleep...");
            try {
                Thread.sleep(1000 * 5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
