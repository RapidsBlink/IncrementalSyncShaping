package com.alibaba.middleware.race.sync.network.NativeSocket;

import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by will on 14/6/2017.
 */
public class NativeServer {
    public static boolean isStoped = false;

    public ServerSocket serverSocket;
    public Socket clientSocket;
    public BufferedWriter outputChannel;
    public BufferedReader inputChannel;


    private Logger logger;
    private ArrayBlockingQueue<String> sendQueue = new ArrayBlockingQueue<>(NetworkConstant.SEND_CHUNK_BUFF_SIZE);

    public static String[] args;
    private int port;
    private ExecutorService sendServicePooledThread = Executors.newSingleThreadExecutor();
    private ExecutorService bossServicePooledThread = Executors.newSingleThreadExecutor();

    public NativeServer(String[] args, int port) {
        this.logger = LoggerFactory.getLogger(NativeServer.class);
        this.port = port;
        this.args = args;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void send(String data) {
        try {
            sendQueue.put(data);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.info(e.getMessage());
            logger.warn("ERROR WHILE PUTTING DATA INTO QUEUE");
        }
    }

    public void start() {
        bossServicePooledThread.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!isStoped) {
                        clientSocket = serverSocket.accept();
                        clientSocket.setKeepAlive(true);
                        clientSocket.setSendBufferSize(NetworkConstant.SEND_BUFF_SIZE);
                        outputChannel = new BufferedWriter(
                                new OutputStreamWriter(new BufferedOutputStream(new SnappyFramedOutputStream(
                                        clientSocket.getOutputStream()), NetworkConstant.SEND_BUFF_SIZE)));
                        inputChannel = new BufferedReader(new InputStreamReader(new BufferedInputStream(new SnappyFramedInputStream(clientSocket.getInputStream()))));

                        String message = inputChannel.readLine();
                        if (message.charAt(0) == NetworkConstant.REQUIRE_ARGS) {
                            logger.info("Received a REQUIRE_ARGS package...");
                            ArgumentsPayloadBuilder argsPayload = new ArgumentsPayloadBuilder(NativeServer.args);
                            outputChannel.write(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, argsPayload.toString()));
                            outputChannel.flush();

                            sendServicePooledThread.execute(new Runnable() {
                                long sendCount = 0;

                                @Override
                                public void run() {
                                    while (true) {
                                        try {
                                            String message = null;
                                            message = sendQueue.take();
                                            if (message != null) {
                                                sendCount++;
                                                if (sendCount % 5000 == 0)
                                                    logger.info("5000 messages have been sent to client...");
                                                try {
                                                    outputChannel.write(message);
                                                    outputChannel.newLine();
                                                } catch (IOException e) {
                                                    e.printStackTrace();
                                                    logger.info("outputChannel write error.");
                                                    logger.info(e.getMessage());
                                                }
                                                if (message.length() <= 3 && message.charAt(0) == NetworkConstant.FINISHED_ALL) {
                                                    logger.info("send FINISHED_ALL package");
                                                    break;
                                                }
                                            }
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                            logger.info("message take error...");
                                            logger.info(e.getMessage());
                                        }

                                    }
                                }
                            });
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    logger.info("socket listen error");
                    logger.info(e.getMessage());
                }
            }
        });
    }

    public void finish() {
        try {
            isStoped = true;
            send(NetworkConstant.FINISHED_ALL + "");
            while (!sendQueue.isEmpty()) {
            }
            ;
            try {
                outputChannel.flush();
                outputChannel.close();
                inputChannel.close();
                clientSocket.close();
            } catch (IOException e) {
                logger.info(e.getMessage());
                logger.info("close error");
                e.printStackTrace();
            }
            sendServicePooledThread.shutdown();
            bossServicePooledThread.shutdown();
            sendServicePooledThread.awaitTermination(5000, TimeUnit.MILLISECONDS);
            bossServicePooledThread.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.info(e.getMessage());
        }
    }
}
