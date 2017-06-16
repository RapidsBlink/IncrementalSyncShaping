package com.alibaba.middleware.race.sync.server2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.server2.FileTransformComputation.CHUNK_SIZE;
import static com.alibaba.middleware.race.sync.server2.FileTransformComputation.writeFilePool;

/**
 * Created by yche on 6/16/17.
 */
public class FileTransformMediator {
    private FileChannel fileChannel;

    // input
    private MappedByteBuffer[] mappedByteBufferArr;
    private int activeMappedBuffer = 0;
    // output
    private BufferedOutputStream bufferedOutputStream;

    private int nextIndex;
    private int maxIndex;   // inclusive
    private int lastChunkLength;

    private Queue<Future<ByteBuffer>> byteBufferFutureQueue; // consumed by output stream


    public FileTransformMediator(String fileName, String srcFolder, String dstFolder) throws IOException {
        // 1st: file length
        String srcFilePath = srcFolder + File.separator + fileName;
        String dstFilePath = dstFolder + File.separator + fileName;

        File file = new File(srcFilePath);
        int fileSize = (int) file.length();
        this.lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;

        // 2nd: index info
        this.maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        this.nextIndex = 0;

        // 3rd: fileChannel for reading with mmap
        this.fileChannel = new RandomAccessFile(srcFilePath, "r").getChannel();
        this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(dstFilePath));
    }

    private void assignTransformTasks(int index, int chunkLength) {
        
    }

    private void assignWriterTask(final ByteBuffer byteBuffer) {
        writeFilePool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    bufferedOutputStream.write(byteBuffer.array(), 0, byteBuffer.limit());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void closeWriterTask() {
        writeFilePool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    bufferedOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void transformFile() {

    }
}
