package com.alibaba.middleware.race.sync.server;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibaba.middleware.race.sync.server.FileUtil.unmap;

/**
 * Created by yche on 6/9/17.
 */
public class ReversedLinesDirectReader {
    private static int CHUNK_SIZE = 32 * 1024 * 1024;
    private static byte LINE_SPLITTER = 0x0a;   // \n
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private int maxIndex;
    private int nextIndex;
    private int lastChunkLength;

    private int inChunkIndex;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(512 * 1024);

    private void initMappedByteBuffer() throws IOException {
        if (nextIndex != maxIndex) {
            unmap(mappedByteBuffer);
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, nextIndex * CHUNK_SIZE, CHUNK_SIZE);
            inChunkIndex = CHUNK_SIZE - 1;
        } else {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, nextIndex * CHUNK_SIZE, lastChunkLength);
            inChunkIndex = lastChunkLength - 1;
        }
        mappedByteBuffer.load();
        nextIndex--;
    }

    public ReversedLinesDirectReader(String filePath) throws IOException {
        this.fileChannel = new RandomAccessFile(filePath, "r").getChannel();
        File file = new File(filePath);
        int fileSize = (int) file.length();
        this.maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        this.lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;

        this.nextIndex = this.maxIndex;
        initMappedByteBuffer();
    }

    private void findFirstValidChar() {
        while (inChunkIndex >= 0 && mappedByteBuffer.get(inChunkIndex) == LINE_SPLITTER) {
            inChunkIndex--;
        }
    }

    private void constructString() throws IOException {
        byte ch;
        while (inChunkIndex >= 0 && (ch = mappedByteBuffer.get(inChunkIndex)) != LINE_SPLITTER) {
            byteBuffer.put(ch);
            inChunkIndex--;
        }

        // if it is not the first char
        if (inChunkIndex == -1 && nextIndex != -1) {
            initMappedByteBuffer();
            constructString();
        }
    }

    public String readLine() throws IOException {
        byte[] bytes = readLineBytes();
        return bytes == null ? null : new String(bytes);
    }

    public byte[] readLineBytes() throws IOException {
        findFirstValidChar();

        if (inChunkIndex < 0) {
            if (nextIndex == -1) {
                return null;
            } else {
                initMappedByteBuffer();
                findFirstValidChar();
            }
        }

        byteBuffer.clear();
        constructString();
        byteBuffer.flip();
        byte[] bytes = new byte[byteBuffer.limit()];
        for (int i = byteBuffer.limit() - 1; i >= 0; i--) {
            bytes[byteBuffer.limit() - 1 - i] = byteBuffer.get(i);
        }

        return bytes;
    }
}
