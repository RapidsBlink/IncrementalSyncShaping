package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.concurrent.Callable;

/**
 * Created by yche on 6/18/17.
 * used by transform thread pool
 */
public class FileTransformTask implements Callable<HashMap<Long, RecordOperation>> {
    // functionality
    private RecordScanner backupScanner;
    private final RecordScanner recordScanner;
    private final TransformComputation transformComputation = new TransformComputation();

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
        this.recordScanner = new RecordScanner(mappedByteBuffer, startIndex, endIndex, transformComputation);
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer) {
        this(mappedByteBuffer, startIndex, endIndex);
        backupScanner = new RecordScanner(remainingByteBuffer, 0, remainingByteBuffer.limit(), transformComputation);
    }

    @Override
    public HashMap<Long, RecordOperation> call() throws Exception {
        if (backupScanner != null) {
            backupScanner.compute();
        }
        recordScanner.compute();

        // make it ready for others to read
        return transformComputation.recordOperationHashMap;
    }
}