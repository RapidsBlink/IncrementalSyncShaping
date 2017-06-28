package com.alibaba.middleware.race.sync.NioSocket;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by will on 24/6/2017.
 */
public class NioServer {
    private byte FINISHED_ALL = 'F';
    private byte REQUIRE_ARGS = 'A';
//    public Logger logger = null;
    private int port;
    private String[] args;
    private ArrayBlockingQueue<ByteBuffer> sendQueue = new ArrayBlockingQueue<ByteBuffer>(8);

    private boolean finished = false;

    private ServerSocketChannel serverChannel;
    private SocketChannel clientChannel;

    private ExecutorService serverThreadsPool = Executors.newSingleThreadExecutor();

    public NioServer(String[] args, int port) {
//        this.logger = LoggerFactory.getLogger(NioServer.class);
        this.port = port;
        this.args = args;
        try {
            this.serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        serverThreadsPool.execute(new Runnable() {
            @Override
            public void run() {
                ByteBuffer chunkSize = ByteBuffer.allocate(4);
                Thread.currentThread().setName("Server-networking-threads");
                try {
                    serverChannel.socket().bind(new InetSocketAddress(port));
//                    logger.info("server started......");

                    clientChannel = serverChannel.accept();
                    clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
//                    logger.info("client connected.....");

                    ByteBuffer readBuff = ByteBuffer.allocate(1);
                    clientChannel.read(readBuff);
                    if (readBuff.get(0) == REQUIRE_ARGS) {
                        ByteBuffer argsBuff = ByteBuffer.wrap(new ArgumentsPayloadBuilder(args).toString().getBytes());
                        chunkSize.clear();
//                        logger.info("data chunk size: " + argsBuff.limit());
                        chunkSize.putInt(argsBuff.limit());
                        chunkSize.flip();
                        clientChannel.write(chunkSize);
                        clientChannel.write(argsBuff);
                        while (true) {
                            try {
                                ByteBuffer data = sendQueue.take();
                                if (data.limit() == 1 && data.get(0) == FINISHED_ALL) {
                                    clientChannel.finishConnect();
                                    clientChannel.close();
                                    finished = true;
                                    break;
                                } else {
                                    chunkSize.clear();
//                                    logger.info("data chunk size: " + data.limit());
                                    chunkSize.putInt(data.limit());
                                    chunkSize.flip();
                                    clientChannel.write(chunkSize);
                                    clientChannel.write(data);
                                }

                            } catch (InterruptedException e) {
                                e.printStackTrace();
//                                logger.info(e.getMessage());
                            }
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }


            }
        });
    }

    public void send(ByteBuffer data) {
        try {
            sendQueue.put(data);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void finish() {
        try {
            sendQueue.put(ByteBuffer.wrap("F".getBytes()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            while (!finished) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            serverThreadsPool.shutdown();
            serverThreadsPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
