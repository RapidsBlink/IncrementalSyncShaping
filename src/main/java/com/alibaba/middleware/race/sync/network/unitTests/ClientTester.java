package com.alibaba.middleware.race.sync.network.unitTests;

import com.alibaba.middleware.race.sync.NioSocket.NioClient;
import com.alibaba.middleware.race.sync.NioSocket.NioServer;
import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NativeSocket.NativeClient;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Created by will on 7/6/2017.
 */
public class ClientTester {
    public static void main(String[] args) {
        Server.initProperties();
        NioClient client = new NioClient("127.0.0.1", 8080);
        FileChannel fch = null;
        try {
            fch= new RandomAccessFile("./tmp.txt", "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        client.start(fch);

        try {
            fch.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
