package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.server.FileUtil.unmap;
import static com.alibaba.middleware.race.sync.server2.FileTransformComputation.CHUNK_SIZE;
import static com.alibaba.middleware.race.sync.server2.FileTransformComputation.TRANSFORM_WORKER_NUM;
import static com.alibaba.middleware.race.sync.server2.FileTransformComputation.writeFilePool;

/**
 * Created by yche on 6/16/17.
 */
public class FileTransformWriteMediator {
    private FileChannel fileChannel;

    // input
    private MappedByteBuffer mappedByteBuffer;
    // output
    private BufferedOutputStream bufferedOutputStream;

    private int nextIndex;
    private int maxIndex;   // inclusive
    private int lastChunkLength;
    private int currChunkLength;

    private Queue<Future<ByteBuffer>> byteBufferFutureQueue; // consumed by output stream

    private ByteBuffer prevRemainingBytes = ByteBuffer.allocate(32 * 1024);

    public FileTransformWriteMediator(String fileName, String srcFolder, String dstFolder) throws IOException {
        // 1st: file length
        String srcFilePath = srcFolder + File.separator + fileName;
        String dstFilePath = dstFolder + File.separator + fileName;

        File file = new File(srcFilePath);
        int fileSize = (int) file.length();
        this.lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;

        // 2nd: index info
        this.maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;

        // 3rd: fileChannel for reading with mmap
        this.fileChannel = new RandomAccessFile(srcFilePath, "r").getChannel();
        this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(dstFilePath));
    }

    // 1st work
    private void fetchNextMmapChunk() throws IOException {
        currChunkLength = nextIndex != maxIndex ? CHUNK_SIZE : lastChunkLength;

        if (mappedByteBuffer != null) {
            unmap(mappedByteBuffer);
        }
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, nextIndex * CHUNK_SIZE, currChunkLength);
        mappedByteBuffer.load();
    }

    // 2nd work: merge remaining, compute [start, end)
    private void assignTransformTasks() {
        // previous tail, should be copied into task
        int avgTask = currChunkLength / TRANSFORM_WORKER_NUM;
        if (prevRemainingBytes.position() > 0) {

        }

        // current tail, clear and then put
        prevRemainingBytes.clear();
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

    // 3rd work
    private void assignWriterTasks() {
        while (!byteBufferFutureQueue.isEmpty()) {
            Future<ByteBuffer> future = byteBufferFutureQueue.peek();
            ByteBuffer result = null;
            while (result == null) {
                try {
                    result = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            assignWriterTask(result);
        }
    }

    private void oneChunkComputation() {
        try {
            fetchNextMmapChunk();
        } catch (IOException e) {
            e.printStackTrace();
            if (Server.logger != null) {
                Server.logger.info(e.getMessage());
            }
        }
        assignTransformTasks();
        assignWriterTasks();
    }

    private void finish() {
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
        unmap(mappedByteBuffer);
    }

    public void transformFile() {
        // do chunk transform
        for (nextIndex = 0; nextIndex <= maxIndex; nextIndex++) {
            oneChunkComputation();
        }

        // close stream, and unmap
        finish();
    }
}
