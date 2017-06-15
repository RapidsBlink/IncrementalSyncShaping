package com.alibaba.middleware.race.sync.network.NativeSocket;

import com.alibaba.middleware.race.sync.client.ClientComputation;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
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


    public NativeClient(String hostName, int port) {
        logger = LoggerFactory.getLogger(NativeClient.class);
        this.hostName = hostName;
        this.port = port;


    }

    private void establishConnection() {
        while (true) {
            clientSocket = new Socket();
            try {
                clientSocket.connect(new InetSocketAddress(hostName, port), 1000);
                clientSocket.setKeepAlive(true);
                clientSocket.setReceiveBufferSize(NetworkConstant.SEND_BUFF_SIZE);
                clientSocket.setTcpNoDelay(true);
                inputChannel = new BufferedReader(
                        new InputStreamReader(new BufferedInputStream(new SnappyFramedInputStream(
                                clientSocket.getInputStream()), NetworkConstant.SEND_BUFF_SIZE)));
                outputChannel = new BufferedWriter(
                        new OutputStreamWriter(new BufferedOutputStream(new SnappyFramedOutputStream(clientSocket.getOutputStream()))));
                break;
            } catch (IOException e) {
                logger.info("connect failed... reconnecting");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(3000);
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
                e.printStackTrace();
            }
            clientSocket = null;
        }
    }

    public void start() {
        establishConnection();
        logger.info("Connection established...");
        try {
            outputChannel.write(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, ""));
            outputChannel.flush();
            String message = inputChannel.readLine();
            if (message.charAt(0) == NetworkConstant.REQUIRE_ARGS) {
                logger.info("Received a REQUIRE_ARGS message...");
                logger.info(Arrays.toString(new ArgumentsPayloadBuilder(message.substring(1)).args));

                receivePooledThread.execute(new Runnable() {
                    int recvCount = 0;

                    @Override
                    public void run() {
                        while (true) {
                            try {
                                String message = inputChannel.readLine();
                                recvCount++;
                                if (recvCount % 5000 == 0) {
                                    logger.info("received 5000 messages");
                                }
                                if (message.length() <= 3 && message.charAt(0) == NetworkConstant.FINISHED_ALL) {
                                    logger.info("Received a FINISHED_ALL package, exit...");
                                    finishLock.lock();
                                    isStoped = true;
                                    finishCondition.signal();
                                    finishLock.unlock();
                                    break;
                                } else {
                                    try {
                                        long pk = ClientComputation.extractPK(message);
                                        resultMap.put(pk, message);
                                    } catch (NumberFormatException e) {
                                        logger.info(e.getMessage());
                                        logger.info("decoding error");
                                    }

                                }

                            } catch (IOException e) {
                                logger.info(e.getMessage());
                                logger.info("message readline error...");
                                e.printStackTrace();
                            }
                        }
                    }
                });

            }
        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }
    }

    public void finish() {
        while (!isStoped) {
            finishLock.lock();
            if (!isStoped) {
                try {
                    finishCondition.await();
                } catch (InterruptedException e) {
                    logger.info(e.getMessage());
                    e.printStackTrace();
                }
            }
            finishLock.unlock();
        }

        receivePooledThread.shutdown();
        try {
            receivePooledThread.awaitTermination(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.info("receivePooledThread.awaitTermination error.");
            e.printStackTrace();
            logger.info(e.getMessage());
        }
        try {
            outputChannel.close();
            inputChannel.close();
            clientSocket.close();
        } catch (IOException e) {
            logger.info("close error.");
            e.printStackTrace();
            logger.info(e.getMessage());
        }
    }

}
