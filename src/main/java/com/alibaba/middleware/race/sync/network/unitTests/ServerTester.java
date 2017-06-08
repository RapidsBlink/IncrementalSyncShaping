package com.alibaba.middleware.race.sync.network.unitTests;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NettyServer;

/**
 * Created by will on 7/6/2017.
 */
public class ServerTester {
    public static void main(String[] args){
        Server.initProperties();
        new NettyServer(8080, "/tmp/test/canal_data").start();
    }
}
