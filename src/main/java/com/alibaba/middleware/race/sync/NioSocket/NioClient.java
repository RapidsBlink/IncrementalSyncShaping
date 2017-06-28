package com.alibaba.middleware.race.sync.NioSocket;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
//import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by will on 24/6/2017.
 */
public class NioClient {
//    public Logger logger;
    private String hostName;
    private int port;
    private SocketChannel clientChannel;

    private ByteBuffer recvSizeBuff = ByteBuffer.allocate(4);// always use int size

    public NioClient(String hostName, int port){
//        logger = LoggerFactory.getLogger(NioClient.class);
        this.hostName = hostName;
        this.port = port;
        establishConnection();
    }

    private void establishConnection(){
        while(true) {
            try {
                clientChannel = SocketChannel.open();
                clientChannel.configureBlocking(true);
                clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                clientChannel.connect(new InetSocketAddress(hostName, port));
                break;
            } catch (IOException e) {
//                logger.info("server not ready, reconnecting.....");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private int recvChunkSize() throws IOException {
        recvSizeBuff.clear();
        int recvCount = 0;
        while(recvCount < 4){
            int rc = clientChannel.read(recvSizeBuff);
            if(rc < 0)
                break;
            recvCount+=rc;
        }

        recvSizeBuff.rewind();
        return recvSizeBuff.getInt();
    }

    public void start(FileChannel outputFile){
        if(outputFile == null){
//            logger.info("output file should not be null......");
            return;
        }
        try {
            clientChannel.write(ByteBuffer.wrap("A".getBytes()));
            int chunkSize = recvChunkSize();
//            logger.info("received a chunk with size: " + chunkSize);
            int recvCount = 0;
            ByteBuffer recvBuff = ByteBuffer.allocate(chunkSize);
            while (recvCount < chunkSize){
                recvCount += clientChannel.read(recvBuff);
            }
            String[] args = new ArgumentsPayloadBuilder(new String(recvBuff.array(), 0, chunkSize)).args;

//            logger.info(Arrays.toString(args));

            chunkSize = recvChunkSize();

            outputFile.transferFrom(clientChannel, 0, chunkSize);

            clientChannel.finishConnect();
            clientChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
//            logger.info(e.getMessage());
        }
    }

}
