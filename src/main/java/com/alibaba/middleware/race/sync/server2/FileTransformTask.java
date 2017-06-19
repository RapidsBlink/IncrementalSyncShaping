package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * Created by yche on 6/18/17.
 * used by transform thread pool
 */
public class FileTransformTask implements Callable<ArrayList<RecordKeyValuePair>> {
    // functionality
    private RecordScanner backupScanner;
    private final RecordScanner recordScanner;

    // result
    private final ArrayList<RecordKeyValuePair> retRecordWrapperArrayList; // fast-consumption object

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
        this.retRecordWrapperArrayList = new ArrayList<>();
        this.recordScanner = new RecordScanner(mappedByteBuffer, startIndex, endIndex,  this.retRecordWrapperArrayList);
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer) {
        this(mappedByteBuffer, startIndex, endIndex);
        backupScanner = new RecordScanner(remainingByteBuffer, 0, remainingByteBuffer.limit(),this.retRecordWrapperArrayList);
    }

    @Override
    public ArrayList<RecordKeyValuePair> call() throws Exception {
        if (backupScanner != null) {
            backupScanner.compute();
        }
        recordScanner.compute();

        // make it ready for others to read
        return  retRecordWrapperArrayList;
    }
}