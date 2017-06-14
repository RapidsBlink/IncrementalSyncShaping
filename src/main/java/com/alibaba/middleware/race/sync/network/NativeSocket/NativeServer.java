package com.alibaba.middleware.race.sync.network.NativeSocket;

import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;
import com.alibaba.middleware.race.sync.network.TransferClass.NetworkStringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private String[] args;
    private int port;
    private ExecutorService sendServicePooledThread = Executors.newSingleThreadExecutor();

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
            logger.warn("ERROR WHILE PUTTING DATA INTO QUEUE");
        }
    }

    public void start() {
        try {
            if (!isStoped) {
                clientSocket = serverSocket.accept();
                clientSocket.setKeepAlive(true);
                clientSocket.setSendBufferSize(NetworkConstant.SEND_BUFF_SIZE);
                outputChannel = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()), NetworkConstant.SEND_BUFF_SIZE);
                inputChannel = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                String message = inputChannel.readLine();
                if (message.charAt(0) == NetworkConstant.REQUIRE_ARGS) {
                    logger.info("Received a REQUIRE_ARGS package...");
                    ArgumentsPayloadBuilder argsPayload = new ArgumentsPayloadBuilder(this.args);
                    outputChannel.write(NetworkStringMessage.buildMessage(NetworkConstant.REQUIRE_ARGS, argsPayload.toString()));
                    outputChannel.flush();

                    sendServicePooledThread.execute(new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                try {
                                    String message = sendQueue.take();
                                    //logger.info("has message, send to client...");
                                    try {
                                        outputChannel.write(message);
                                        outputChannel.newLine();
                                    } catch (IOException e1) {
                                        e1.printStackTrace();
                                    }
                                    if (message.length() <= 3 && message.charAt(0) == NetworkConstant.FINISHED_ALL) {
                                        logger.info("send FINISHED_ALL package");
                                        break;
                                    }
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    });
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
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
                e.printStackTrace();
            }
            sendServicePooledThread.shutdown();
            sendServicePooledThread.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
