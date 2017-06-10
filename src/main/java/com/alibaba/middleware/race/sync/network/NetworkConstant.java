package com.alibaba.middleware.race.sync.network;

/**
 * Created by will on 7/6/2017.
 */
public interface NetworkConstant {
    int MAX_CHUNK_SIZE = 100 * 1024 * 1024; // 100MB
    int SEND_CHUNK_BUFF_SIZE = 1000000;
    String END_OF_TRANSMISSION = "\r\n";

    char FINISHED_ALL = 'F';
    char REQUIRE_ARGS = 'A';
    char LINE_RECORD = 'B';
}
