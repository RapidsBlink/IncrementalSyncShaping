package com.alibaba.middleware.race.sync.server2;


import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by yche on 6/18/17.
 * used by transform thread pool
 */
public class FileTransformTask implements Runnable {
    // functionality
    private RecordScanner backupScanner;
    private final RecordScanner recordScanner;

    // result

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, Future<?> prevFuture) {
        this.recordScanner = new RecordScanner(mappedByteBuffer, startIndex, endIndex, prevFuture);
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer, Future<?> prevFuture) {
        this(mappedByteBuffer, startIndex, endIndex, prevFuture);
        backupScanner = new RecordScanner(remainingByteBuffer, 0, remainingByteBuffer.limit(), prevFuture);
    }

    @Override
    public void run() {
        if (backupScanner != null) {
            try {
                backupScanner.compute();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        try {
            recordScanner.compute();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}