package com.alibaba.middleware.race.sync.unused;

import com.alibaba.middleware.race.sync.unused.server.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yche on 6/16/17.
 * not thread safe
 */
public class LineDirectReader {
    private static int CHUNK_SIZE = 64 * 1024 * 1024;
    private static byte LINE_SPLITTER = '\n';

    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private int maxChunkIndex;
    private int nextChunkIndex;

    private int maxChunkLength;
    private final int lastChunkLength;

    private int inChunkIndex;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(16 * 1024);

    public LineDirectReader(String filePath) throws IOException {
        this.fileChannel = new RandomAccessFile(filePath, "r").getChannel();
        File file = new File(filePath);
        int fileSize = (int) file.length();
        this.maxChunkIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        this.lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;
        //System.out.println("lastChunkLength:" + lastChunkLength);

        // fetch the first chunk
        this.nextChunkIndex = 0;
        fetchNextChunk();
    }

    private void fetchNextChunk() throws IOException {
        // 1st: set chunk size
        if (nextChunkIndex > maxChunkIndex) {
            System.out.println("in chunk index:" + inChunkIndex);
            System.out.println("maxChunkLength:" + maxChunkLength);
            System.out.println("next chunk index" + nextChunkIndex);
        }

        maxChunkLength = nextChunkIndex != maxChunkIndex ? CHUNK_SIZE : lastChunkLength;

        //System.out.println("max chunk len:" + maxChunkLength + ", cur next chunk index:" + nextChunkIndex + ", max index:" + maxChunkIndex);
        // 2nd: load memory
        if (mappedByteBuffer != null) {
            FileUtil.unmap(mappedByteBuffer);
        }
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, nextChunkIndex * CHUNK_SIZE, maxChunkLength);
        mappedByteBuffer.load();

        // 3rd: update parameters
        inChunkIndex = 0;
        nextChunkIndex++;
    }

    private byte peekNextByte() throws IOException {
        if (inChunkIndex >= maxChunkLength) {
            fetchNextChunk();
        }
        return mappedByteBuffer.get(inChunkIndex);
    }

    private void skipOneStringBytes() throws IOException {
        byte FIELD_SPLITTER = '|';
        while (peekNextByte() == LINE_SPLITTER || peekNextByte() == FIELD_SPLITTER)
            inChunkIndex++;

        while (peekNextByte() != FIELD_SPLITTER) {
            inChunkIndex++;
        }
    }

    private byte[] getLineBytes() throws IOException {
        byte nextByte;
        byteBuffer.clear();
        inChunkIndex++;
        while ((nextByte = peekNextByte()) != LINE_SPLITTER) {
            byteBuffer.put(nextByte);
            inChunkIndex++;
            // reach the end
            if (nextChunkIndex >= maxChunkIndex && inChunkIndex >= maxChunkLength) {
                //System.out.println("exit this round");
                break;
            }
        }

        byteBuffer.flip();

        byte[] retBytes = new byte[byteBuffer.limit() + 1];
        System.arraycopy(byteBuffer.array(), 0, retBytes, 0, byteBuffer.limit());
        retBytes[byteBuffer.limit()] = '\n';
        return retBytes;
    }

    public byte[] readLineBytes() throws IOException {
        // the last chunk finished
        // avoid the last is '\n'
        if (nextChunkIndex >= maxChunkIndex && inChunkIndex >= maxChunkLength - 1) {
            FileUtil.unmap(mappedByteBuffer);
            return null;
        }

        // 1st, skip: binlog id, timestamp, schema and table
        for (int i = 0; i < 4; i++) {
            skipOneStringBytes();
        }
        if (nextChunkIndex >= maxChunkIndex && maxChunkLength - inChunkIndex < 1000) {
            System.out.println(new String(getLineBytes()));
        }
        return getLineBytes();
    }
}
