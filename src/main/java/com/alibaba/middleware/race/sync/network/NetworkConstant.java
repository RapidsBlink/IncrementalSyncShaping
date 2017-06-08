package com.alibaba.middleware.race.sync.network;

/**
 * Created by will on 7/6/2017.
 */
public interface NetworkConstant {
    int MAX_CHUNK_SIZE = 100 * 1024 * 1024; // 100MB
    String END_OF_TRANSMISSION = "\r\n";
    char REQUIRE_ARGS = 0x01;
}
