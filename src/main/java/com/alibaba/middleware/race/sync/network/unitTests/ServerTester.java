package com.alibaba.middleware.race.sync.network.unitTests;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NativeSocket.NativeServer;

/**
 * Created by will on 7/6/2017.
 */
public class ServerTester {
    public static void main(String[] args) {
        Server.initProperties();
        NativeServer ns = new NativeServer(args, 8080);
        ns.start();
        for (int i = 0; i < 1000000; i++) {
            ns.send(i + "\t" + "\t张\t三\t男\t382\t583");
        }
        ns.finish();
    }
}
