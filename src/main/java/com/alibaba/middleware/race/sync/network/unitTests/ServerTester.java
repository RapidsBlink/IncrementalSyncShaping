package com.alibaba.middleware.race.sync.network.unitTests;

import com.alibaba.middleware.race.sync.NioSocket.NioServer;
import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NativeSocket.NativeServer;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;

import java.nio.ByteBuffer;

/**
 * Created by will on 7/6/2017.
 */
public class ServerTester {
    public static void main(String[] args) {
        Server.initProperties();
        NioServer ns = new NioServer(args, 8080);
        ns.start();

        ns.send(ByteBuffer.wrap(new String("hello,world\nhello,world\thello in one line.\n").getBytes()));

        ns.finish();

    }
}
