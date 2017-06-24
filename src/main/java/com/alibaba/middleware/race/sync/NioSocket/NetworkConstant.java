package com.alibaba.middleware.race.sync.NioSocket;

/**
 * Created by will on 7/6/2017.
 */
public interface NetworkConstant {
    int SEND_BUFF_SIZE = 1 * 1024 * 1024;
    int MAX_CHUNK_SIZE = 100 * 1024 * 1024; // 100MB
    int SEND_CHUNK_BUFF_SIZE = 1000000;
    String END_OF_TRANSMISSION = "\n";
    byte END_OF_MESSAGE = '`';

    byte FINISHED_ALL = 'F';
    byte REQUIRE_ARGS = 'A';
    byte LINE_RECORD = 'B';
    byte BUFFERED_RECORD = 'C';
}
