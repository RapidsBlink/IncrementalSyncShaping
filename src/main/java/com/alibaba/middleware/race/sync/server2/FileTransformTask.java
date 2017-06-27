package com.alibaba.middleware.race.sync.server2;


import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yche on 6/18/17.
 * used by transform thread pool
 */
public class FileTransformTask implements Callable<RecordScanner> {
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
    // 1st
    private MappedByteBuffer mappedByteBuffer;
    private int startIndex;
    private int endIndex;
    // extra
    private final ExtraTaskInfo taskInfo;

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        taskInfo = null;
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
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
    public RecordScanner call() throws Exception {
        fetchMyRecordScanner();
        recordScanner.clear();
        if (taskInfo != null) {
            recordScanner.reuse(taskInfo.mappedByteBuffer, taskInfo.startIndex, taskInfo.endIndex);
            recordScanner.compute();
            recordScanner.reuse(mappedByteBuffer, startIndex, endIndex);
            recordScanner.compute();
        } else {
            recordScanner.reuse(mappedByteBuffer, startIndex, endIndex);
            recordScanner.compute();
        }
        return recordScanner;
    }
}