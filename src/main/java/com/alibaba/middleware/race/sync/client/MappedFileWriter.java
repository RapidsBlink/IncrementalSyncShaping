package com.alibaba.middleware.race.sync.client;

/**
 * Created by yche on 6/23/17.
 */
import com.alibaba.middleware.race.sync.server2.FileUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by will on 16/6/2017.
 */
public class MappedFileWriter {

    private static int CHUNK_SIZE = 48 * 1024 * 1024;

    private FileChannel fileChannel;
    private long realSize = 0;
    private int currentInnerChunkIndex = 0;
    private MappedByteBuffer mappedByteBuffer = null;

    public MappedFileWriter(String fullFileName, long expectLength) throws IOException {
        fileChannel = new RandomAccessFile(fullFileName, "rw").getChannel();
        fileChannel.truncate(expectLength);
        getNextChunk();
    }

    public void write(byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    public void write(byte data) throws IOException {
        mappedByteBuffer.put(data);
        realSize++;
        currentInnerChunkIndex++;
        if(currentInnerChunkIndex == CHUNK_SIZE)
            getNextChunk();
    }

    public void write(byte[] data, int offset, int length) throws IOException {
        if(length <= 0 )
            return;
        int expectEnd = currentInnerChunkIndex + length;
        if (expectEnd <= CHUNK_SIZE) {
            mappedByteBuffer.put(data, offset, length);
            currentInnerChunkIndex = expectEnd;
            realSize += length;
        } else {
            int writeLength = CHUNK_SIZE - currentInnerChunkIndex;
            mappedByteBuffer.put(data, offset, writeLength);
            realSize+=writeLength;
            getNextChunk();
            write(data, offset + writeLength, length - writeLength);
        }
        if (currentInnerChunkIndex == CHUNK_SIZE)
            getNextChunk();
    }

    public void close() throws IOException {
        if (mappedByteBuffer != null) {
            FileUtil.unmap(mappedByteBuffer);
        }
        fileChannel.truncate(realSize);
        fileChannel.close();
    }

    public void close(int reduceSize) throws IOException {
        if (mappedByteBuffer != null) {
            FileUtil.unmap(mappedByteBuffer);
        }
        fileChannel.truncate(realSize - reduceSize);
        fileChannel.close();
    }

    public void getNextChunk() throws IOException {
        if (mappedByteBuffer != null) {
            FileUtil.unmap(mappedByteBuffer);
        }
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, realSize, CHUNK_SIZE);
        mappedByteBuffer.load();
        currentInnerChunkIndex = 0;
    }
}