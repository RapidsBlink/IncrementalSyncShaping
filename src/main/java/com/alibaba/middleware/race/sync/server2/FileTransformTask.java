package com.alibaba.middleware.race.sync.server2;


import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by yche on 6/18/17.
 * used by transform thread pool
 */
public class FileTransformTask implements Runnable {
    // functionality
    private static ConcurrentHashMap<Long, RecordScanner> recordScannerConcurrentHashMap = new ConcurrentHashMap<>();

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

    private RecordScanner recordScanner;
    private int startIndex;
    private int endIndex;
    private ByteBuffer mappedByteBuffer;
    private Future<?> prevFuture;

    private ExtraTaskInfo taskInfo;

    // result

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, Future<?> prevFuture) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.prevFuture = prevFuture;
        taskInfo = null;
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer, Future<?> prevFuture) {
        this(mappedByteBuffer, startIndex, endIndex, prevFuture);
        taskInfo = new ExtraTaskInfo(remainingByteBuffer, 0, remainingByteBuffer.limit());
    }

    private void fetchMyRecordScanner() {
        long myId = Thread.currentThread().getId();
        if (!recordScannerConcurrentHashMap.containsKey(myId)) {
            recordScannerConcurrentHashMap.put(myId, new RecordScanner());
        }
        recordScanner = recordScannerConcurrentHashMap.get(myId);
    }

    @Override
    public void run() {
        fetchMyRecordScanner();
        if (taskInfo != null) {
            try {
                recordScanner.reuse(taskInfo.mappedByteBuffer, taskInfo.startIndex, taskInfo.endIndex);
                recordScanner.compute();
                recordScanner.reuse(mappedByteBuffer, startIndex, endIndex, prevFuture);
                recordScanner.compute();
                recordScanner.waitForSend();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            try {
                recordScanner.reuse(mappedByteBuffer, startIndex, endIndex, prevFuture);
                recordScanner.compute();
                recordScanner.waitForSend();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

    }
}