package com.alibaba.middleware.race.sync.network.unitTests;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.network.NettyServer;
import com.alibaba.middleware.race.sync.network.NetworkConstant;
import com.alibaba.middleware.race.sync.network.TransferClass.ArgumentsPayloadBuilder;

/**
 * Created by will on 7/6/2017.
 */
public class ServerTester {
    public static void main(String[] args) {
        Server.initProperties();
        NettyServer ns = new NettyServer(args, 8080);
        ns.start();
        for (int i = 0; i < 100000; i++) {
            ns.send(NetworkConstant.REQUIRE_ARGS, new ArgumentsPayloadBuilder(args).toString());
        }
        ns.finish();
        ns.stop();
    }
}
