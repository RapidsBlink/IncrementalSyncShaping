package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.network.NettyClient;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by will on 6/6/2017.
 */
public class Client {

    public static Logger logger;

    private final static int port = Constants.SERVER_PORT;
    static NettyClient nettyClient= null;

    public static void main(String[] args){
        new Client(args[0]).start();
    }

    public Client(String ip){
        initProperties();
        logger = LoggerFactory.getLogger(Client.class);
        nettyClient = new NettyClient(ip, Constants.SERVER_PORT);
        nettyClient.start();

    }

    public void start(){
        nettyClient.waitReceiveFinish();
        nettyClient.stop();
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(Constants.RESULT_HOME + File.separator + Constants.RESULT_FILE_NAME));

            for(String value : NettyClient.resultMap.values()){
                logger.info(value);
                bw.write(value);
                bw.newLine();
            }
            bw.close();

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
