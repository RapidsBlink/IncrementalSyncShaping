package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.server.FileUtil.unmap;
import static com.alibaba.middleware.race.sync.server2.FileTransformComputation.*;

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

    private Queue<Future<ByteBuffer>> byteBufferFutureQueue = new LinkedList<>(); // consumed by output stream

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

    // previous tail, should be copied into task
    private int preparePrevBytes() {
        int end = 0;

        if (prevRemainingBytes.position() > 0 && prevRemainingBytes.get(prevRemainingBytes.position() - 1) != LINE_SPLITTER) {
            byte myByte;
            // stop at `\n`
            while ((myByte = mappedByteBuffer.get(end)) != LINE_SPLITTER) {
                prevRemainingBytes.put(myByte);
                end++;
            }
            prevRemainingBytes.put(myByte);
            end++;
        }
        prevRemainingBytes.flip();
        return end;
    }

    private int computeEnd(int smallChunkLastIndex) {
        int end = smallChunkLastIndex;
        while (mappedByteBuffer.get(end) != LINE_SPLITTER) {
            end--;
        }
        end += 1;
        return end;
    }

    // 2nd work: merge remaining, compute [start, end)
    private void assignTransformTasks() {
        int avgTask = currChunkLength / TRANSFORM_WORKER_NUM;

        // index pair
        int start;
        int end = preparePrevBytes();

        // 1st: first worker
        start = end;
        end = computeEnd(avgTask - 1);
        FileTransformTask fileTransformTask;
        if (prevRemainingBytes.limit() > 0) {
            ByteBuffer tmp = ByteBuffer.allocate(prevRemainingBytes.limit());
            tmp.put(prevRemainingBytes);
            fileTransformTask = new FileTransformTask(mappedByteBuffer, start, end, tmp);
        } else {
            fileTransformTask = new FileTransformTask(mappedByteBuffer, start, end);
        }

        byteBufferFutureQueue.add(fileTransformPool.submit(fileTransformTask));

        // 2nd: subsequent workers
        for (int i = 1; i < TRANSFORM_WORKER_NUM; i++) {
            start = end;
            int smallChunkLastIndex = i < TRANSFORM_WORKER_NUM - 1 ? avgTask * (i + 1) - 1 : currChunkLength - 1;
            end = computeEnd(smallChunkLastIndex);
            fileTransformTask = new FileTransformTask(mappedByteBuffer, start, end);
            byteBufferFutureQueue.add(fileTransformPool.submit(fileTransformTask));
        }

        // current tail, clear and then put
        prevRemainingBytes.clear();
        for (int i = end; i < currChunkLength; i++) {
            prevRemainingBytes.put(mappedByteBuffer.get(i));
        }
    }

    private void assignWriterTask(final ByteBuffer byteBuffer) {
        writeFilePool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    bufferedOutputStream.write(byteBuffer.array(), 0, byteBuffer.limit());
                } catch (IOException e) {
                    e.printStackTrace();
                    if (Server.logger != null) {
                        Server.logger.info("assign writer single task exception");
                    }
                }
            }
        });
    }

    // 3rd work
    private void assignWriterTasks() {
        while (!byteBufferFutureQueue.isEmpty()) {
            Future<ByteBuffer> future = byteBufferFutureQueue.poll();
            ByteBuffer result = null;
            try {
                result = future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                if (Server.logger != null) {
                    Server.logger.info("assign task exception");
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
