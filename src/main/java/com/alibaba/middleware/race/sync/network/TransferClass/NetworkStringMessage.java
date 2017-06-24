package com.alibaba.middleware.race.sync.network.TransferClass;

import com.alibaba.middleware.race.sync.network.NetworkConstant;

/**
 * Created by will on 8/6/2017.
 */
public class NetworkStringMessage {
    public static String buildMessage(byte type, String data) {
        return type + data + NetworkConstant.END_OF_TRANSMISSION;
    }

    public static String buildMessageWithoutType(char type, String data) {
        return data + NetworkConstant.END_OF_TRANSMISSION;
    }
}
