package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static com.alibaba.middleware.race.sync.Constants.*;

/**
 * Created by yche on 6/18/17.
 * used for scan the byte arr of record string lines
 */
public class RecordScanner {
    // input
    private final ByteBuffer mappedByteBuffer;
    private final int endIndex;   // exclusive

    // intermediate states
    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(64);
    private final ByteBuffer fieldNameBuffer = ByteBuffer.allocate(128);
    private int nextIndex; // start from startIndex


    // output
    private final ArrayList<LogOperation> recordWrapperArrayList; // fast-consumption object

    public RecordScanner(ByteBuffer mappedByteBuffer, int startIndex, int endIndex,
                         ArrayList<LogOperation> retRecordWrapperArrayList) {
        this.mappedByteBuffer = mappedByteBuffer.asReadOnlyBuffer(); // get a view, with local position, limit
        this.nextIndex = startIndex;
        this.endIndex = endIndex;
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

    private byte[] getNextBytes() {
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER) {
            nextIndex++;
        }

        tmpBuffer.clear();
        byte myByte;
        while ((myByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            tmpBuffer.put(myByte);
            nextIndex++;
        }
        tmpBuffer.flip();
        byte[] myBytes = new byte[tmpBuffer.limit()];
        System.arraycopy(tmpBuffer.array(), 0, myBytes, 0, tmpBuffer.limit());
        return myBytes;
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

    private void skipFieldName() {
        // skip '|'
        nextIndex++;
        // stop at '|'
        byte myByte;
        fieldNameBuffer.clear();
        while ((myByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            fieldNameBuffer.put(myByte);
            nextIndex++;
        }
        fieldNameBuffer.flip();
    }

    private LogOperation scanOneRecord() {
        // 1st: skip: mysql, ts, schema, table
        for (int i = 0; i < 4; i++) {
            skipField();
        }

        // 2nd: parse KeyOperation
        byte operation = mappedByteBuffer.get(nextIndex + 1);
        LogOperation logOperation;
        // skip one splitter and operation byte
        nextIndex += 2;
        skipField();
        if (operation == I_OPERATION) {
            // insert: pre(null) -> cur
            skipField();
            logOperation = InsertOperation.newInsertOperation(getNextLong());
        } else if (operation == D_OPERATION) {
            // delete: pre -> cur(null)
            logOperation = DeleteOperation.newDeleteOperation(getNextLong());
            skipField();
        } else {
            // update
            long prevKey = getNextLong();
            long curKey = getNextLong();
            if (prevKey == curKey) {
                logOperation = UpdateOperation.newUpdateOperation(prevKey);
            } else {
                logOperation = UpdateKeyOperation.newUpdateKeyOperation(prevKey, curKey);
            }
        }

        // 3rd: parse ValueIndex
        // must be insert and update
        if (logOperation instanceof InsertOperation) {
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldName();
                skipField();
                byte[] nextBytes = getNextBytes();
                ((InsertOperation) logOperation).addValue(fieldNameBuffer, nextBytes);
            }
        } else if (logOperation instanceof UpdateOperation) {
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldName();
                skipField();
                byte[] nextBytes = getNextBytes();
                ((UpdateOperation) logOperation).addValue(fieldNameBuffer, nextBytes);
            }
        } else {
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                nextIndex++;
            }
        }

        // skip '|' and `\n`
        nextIndex += 2;
        return logOperation;
    }

    public void compute() {
        while (nextIndex < endIndex) {
            recordWrapperArrayList.add(scanOneRecord());
        }
    }
}
