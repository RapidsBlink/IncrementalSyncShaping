package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * Created by yche on 6/18/17.
 * used by transform thread pool
 */
public class FileTransformTask implements Callable<TransformResultPair> {
    // functionality
    private RecordScanner backupScanner;
    private final RecordScanner recordScanner;

    // result
    private final ByteBuffer retByteBuffer; // fast-consumption object
    private final ArrayList<RecordKeyValuePair> retRecordWrapperArrayList; // fast-consumption object

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
        this.retByteBuffer = ByteBuffer.allocate(endIndex - startIndex);
        this.retRecordWrapperArrayList = new ArrayList<>();
        this.recordScanner = new RecordScanner(mappedByteBuffer, startIndex, endIndex, this.retByteBuffer, this.retRecordWrapperArrayList);
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer) {
        this(mappedByteBuffer, startIndex, endIndex);
        backupScanner = new RecordScanner(remainingByteBuffer, 0, remainingByteBuffer.limit(), this.retByteBuffer, this.retRecordWrapperArrayList);
    }

    @Override
    public TransformResultPair call() throws Exception {
        if (backupScanner != null) {
            backupScanner.compute();
        }
        recordScanner.compute();

        // make it ready for others to read
        retByteBuffer.flip();
        return new TransformResultPair(retByteBuffer, retRecordWrapperArrayList);
    }
}