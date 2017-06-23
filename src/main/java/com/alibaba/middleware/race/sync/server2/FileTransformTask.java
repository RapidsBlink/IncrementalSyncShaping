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
    private static class ExtraTaskInfo {
        int startIndex;
        int endIndex;
        ByteBuffer mappedByteBuffer;

        ExtraTaskInfo(ByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.mappedByteBuffer = mappedByteBuffer;
        }
    }

    private final RecordScanner recordScanner;
    private final ExtraTaskInfo taskInfo;

    // result

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, Future<?> prevFuture) {
        this.recordScanner = new RecordScanner(mappedByteBuffer, startIndex, endIndex, prevFuture);
        taskInfo = null;
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer, Future<?> prevFuture) {
        recordScanner = new RecordScanner(remainingByteBuffer, 0, remainingByteBuffer.limit(), prevFuture);
        taskInfo = new ExtraTaskInfo(mappedByteBuffer, startIndex, endIndex);
    }

    @Override
    public void run() {
        if (taskInfo != null) {
            try {
                recordScanner.compute();
                recordScanner.reuse(taskInfo.mappedByteBuffer, taskInfo.startIndex, taskInfo.endIndex);
                recordScanner.compute();
                recordScanner.waitForSend();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            try {
                recordScanner.compute();
                recordScanner.waitForSend();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

    }
}