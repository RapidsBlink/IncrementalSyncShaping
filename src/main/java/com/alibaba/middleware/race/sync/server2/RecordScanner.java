package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static com.alibaba.middleware.race.sync.Constants.D_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.I_OPERATION;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.FILED_SPLITTER;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.LINE_SPLITTER;

/**
 * Created by yche on 6/18/17.
 */
public class RecordScanner {
    // input
    private final ByteBuffer mappedByteBuffer;
    private final int endIndex;   // exclusive

    // intermediate states
    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(64);
    private int nextIndex; // start from startIndex

    // output
    private final ByteBuffer retByteBuffer; // fast-consumption object
    private final ArrayList<RecordKeyValuePair> recordWrapperArrayList; // fast-consumption object

    RecordScanner(ByteBuffer mappedByteBuffer, int startIndex, int endIndex,
                  ByteBuffer retByteBuffer, ArrayList<RecordKeyValuePair> retRecordWrapperArrayList) {
        this.mappedByteBuffer = mappedByteBuffer.asReadOnlyBuffer(); // get a view, with local position, limit
        this.nextIndex = startIndex;
        this.endIndex = endIndex;
        this.retByteBuffer = retByteBuffer;
        this.recordWrapperArrayList = retRecordWrapperArrayList;
    }

    // stop at `|`
    private void skipField() {
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER) {
            nextIndex++;
        }
        while (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER) {
            nextIndex++;
        }
    }

    private void putIntoByteBufferUntilFieldSplitter() {
        byte myByte;
        while ((myByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            retByteBuffer.put(myByte);
            nextIndex++;
        }
        // add `\n`
        retByteBuffer.put(mappedByteBuffer.get(nextIndex));
        // stop at new start `|`, finally stop at unreachable endIndex
        nextIndex++;
    }

    private long getNextLong() {
        tmpBuffer.clear();
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER)
            nextIndex++;

        byte tmpByte;
        while ((tmpByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            nextIndex++;
            tmpBuffer.put(tmpByte);
        }
        tmpBuffer.flip();

        return Long.valueOf(new String(tmpBuffer.array(), 0, tmpBuffer.limit()));
    }

    private void skipFieldNameAndUpdateStates() {
        int start = nextIndex + 1;
        // skip '|'
        nextIndex++;
        // stop at '|'
        while (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER) {
            nextIndex++;
        }
        int end = nextIndex;

        mappedByteBuffer.position(start);
        mappedByteBuffer.limit(end);
    }

    private RecordKeyValuePair scanOneRecord() {
        // 1st: skip: mysql, ts, schema, table
        for (int i = 0; i < 4; i++) {
            skipField();
        }

        // 2nd: parse KeyOperation
        byte operation = mappedByteBuffer.get(nextIndex + 1);
        // skip one splitter and operation byte
        nextIndex += 2;
        KeyOperation keyOperation = new KeyOperation(operation);
        skipField();
        if (operation == I_OPERATION) {
            // insert: pre(null) -> cur
            skipField();
            keyOperation.curKey(getNextLong());
        } else if (operation == D_OPERATION) {
            // delete: pre -> cur(null)
            keyOperation.preKey(getNextLong());
            skipField();
        } else {
            // update
            keyOperation.preKey(getNextLong());
            keyOperation.curKey(getNextLong());
        }

        // 3rd: parse ValueIndex
        ValueIndexArrWrapper valueIndexArrWrapper = null;
        while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
            if (valueIndexArrWrapper == null) {
                valueIndexArrWrapper = new ValueIndexArrWrapper();
            }

            skipFieldNameAndUpdateStates();
            long curOffset = retByteBuffer.limit();
            skipField();
            putIntoByteBufferUntilFieldSplitter();
            long nextOffset = retByteBuffer.limit();
            valueIndexArrWrapper.addIndex(mappedByteBuffer, curOffset, (short) (nextOffset - curOffset));
        }
        return new RecordKeyValuePair(keyOperation, valueIndexArrWrapper);
    }

    void compute() {
        while (nextIndex < endIndex) {
            recordWrapperArrayList.add(scanOneRecord());
        }
    }
}
