package com.alibaba.middleware.race.sync.network.NativeSocket;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.client.ClientComputation;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by will on 14/6/2017.
 */
public class NativeClient {

    public static ConcurrentMap<Long, String> resultMap = new ConcurrentSkipListMap<>();
    public BufferedWriter outputChannel;
    public BufferedReader inputChannel;
    public Socket clientSocket;

    private static boolean isStoped = false;
    private String hostName;
    private int port;
    private ExecutorService receivePooledThread = Executors.newSingleThreadExecutor();
    private Logger logger;

    private ReentrantLock finishLock = new ReentrantLock();
    private Condition finishCondition = finishLock.newCondition();


    public NativeClient(String hostName, int port){
        logger = LoggerFactory.getLogger(NativeClient.class);
        this.hostName = hostName;
        this.port = port;

        establishConnection();
        logger.info("Connection established...");
    }

    private void establishConnection(){
        clientSocket = new Socket();
            while(true){
                try {
                    clientSocket.connect(new InetSocketAddress(hostName, port), 1000);
                    clientSocket.setKeepAlive(true);
                    clientSocket.setReceiveBufferSize(NetworkConstant.SEND_BUFF_SIZE);
                    clientSocket.setTcpNoDelay(true);
                    inputChannel = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()), NetworkConstant.SEND_BUFF_SIZE);
                    outputChannel = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                    break;
                } catch (IOException e) {
                    logger.info("connect failed... reconnecting");
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
    }
    public void start(){
        try {
            outputChannel.write(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, ""));
            outputChannel.flush();
            String message = inputChannel.readLine();
            if(message.charAt(0) == NetworkConstant.REQUIRE_ARGS){
                logger.info("Received a REQUIRE_ARGS message...");
                logger.info(Arrays.toString(new ArgumentsPayloadBuilder(message.substring(1)).args));

                receivePooledThread.execute(new Runnable() {
                    @Override
                    public void run() {
                        while(true){
                            try {
                                String message = inputChannel.readLine();
                                if(message.length() <=3 && message.charAt(0) == NetworkConstant.FINISHED_ALL){
                                    logger.info("Received a FINISHED_ALL package, exit...");
                                    finishLock.lock();
                                    isStoped = true;
                                    finishCondition.signal();
                                    finishLock.unlock();
                                    break;
                                }else{
                                    try {
                                        long pk = ClientComputation.extractPK(message);
                                        resultMap.put(pk, message);
                                    }catch (NumberFormatException e){
                                        logger.info("decoding error");
                                    }

                                }

                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void finish(){
        while(!isStoped){
            finishLock.lock();
            if(!isStoped){
                try {
                    finishCondition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            finishLock.unlock();
        }

        receivePooledThread.shutdown();
        try {
            receivePooledThread.awaitTermination(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
