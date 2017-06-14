package com.alibaba.middleware.race.sync.network.unitTests;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NativeSocket.NativeClient;
import com.alibaba.middleware.race.sync.network.netty.NettyClient;

/**
 * Created by will on 7/6/2017.
 */
public class ClientTester {
    public static void main(String[] args) {
        Server.initProperties();
        NativeClient client = new NativeClient("127.0.0.1", 8080);
        client.start();

        client.finish();

    }
}
