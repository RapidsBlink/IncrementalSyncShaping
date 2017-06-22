package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.Constants.*;
import static com.alibaba.middleware.race.sync.server2.RecordField.fieldSkipLen;

/**
 * Created by yche on 6/18/17.
 * used for scan the byte arr of record string lines
 */
public class RecordScanner {
    // input
    private final ByteBuffer mappedByteBuffer;
    private final int endIndex;   // exclusive

    // intermediate states
    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(8);
    private int nextIndex; // start from startIndex

    private final ArrayList<LogOperation> localOperations = new ArrayList<>();
    private final Future<?> prevFuture;

    public RecordScanner(ByteBuffer mappedByteBuffer, int startIndex, int endIndex, Future<?> prevFuture) {
        this.mappedByteBuffer = mappedByteBuffer.asReadOnlyBuffer(); // get a view, with local position, limit
        this.nextIndex = startIndex;
        this.endIndex = endIndex;
        this.prevFuture = prevFuture;
    }

    // stop at `|`
    private void skipField() {
        nextIndex++;
        while (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER) {
            nextIndex++;
        }
    }

    private void skipHeader() {
        nextIndex += 20;
        while ((mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            nextIndex++;
        }
        nextIndex += 34;
    }

    private void skipKey() {
        nextIndex += RecordField.KEY_LEN + 1;
    }

    private void skipNull() {
        nextIndex += 5;
    }

    private void skipFieldForInsert(int index) {
        nextIndex += fieldSkipLen[index];
    }

    private void getNextBytesIntoTmp() {
        nextIndex++;

        tmpBuffer.clear();
        byte myByte;
        while ((myByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            tmpBuffer.put(myByte);
            nextIndex++;
        }
        tmpBuffer.flip();
    }

    private long getNextLong() {
        nextIndex++;

        byte tmpByte;
        long result = 0L;
        while ((tmpByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            nextIndex++;
            result = (10 * result) + (tmpByte - '0');
        }
        return result;
    }

    private int skipFieldName() {
        // stop at '|'
        if (mappedByteBuffer.get(nextIndex + 1) == 'f') {
            nextIndex += 15;
            return 0;
        } else if (mappedByteBuffer.get(nextIndex + 1) == 'l') {
            nextIndex += 14;
            return 1;
        } else {
            if (mappedByteBuffer.get(nextIndex + 2) == 'e') {
                nextIndex += 8;
                return 2;
            } else if (mappedByteBuffer.get(nextIndex + 6) == ':') {
                nextIndex += 10;
                return 3;
            } else {
                nextIndex += 11;
                return 4;
            }
        }
    }


    private LogOperation scanOneRecord() {
        // 1st: skip: mysql, ts, schema, table
        skipHeader();

        // 2nd: parse KeyOperation
        byte operation = mappedByteBuffer.get(nextIndex + 1);
        LogOperation logOperation;
        // skip one splitter and operation byte
        nextIndex += 2;
        skipKey();
        if (operation == I_OPERATION) {
            // insert: pre(null) -> cur
            skipNull();
            logOperation = new InsertOperation(getNextLong());
        } else if (operation == D_OPERATION) {
            // delete: pre -> cur(null)
            logOperation = new DeleteOperation(getNextLong());
            skipNull();
        } else {
            // update
            long prevKey = getNextLong();
            long curKey = getNextLong();
            if (prevKey == curKey) {
                logOperation = new UpdateOperation(prevKey);
            } else {
                logOperation = new UpdateKeyOperation(prevKey, curKey);
            }
        }

        // 3rd: parse ValueIndex
        if (logOperation instanceof InsertOperation) {
            int localIndex = 0;
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldForInsert(localIndex);
                skipNull();
                getNextBytesIntoTmp();
                ((InsertOperation) logOperation).addData(localIndex, tmpBuffer);
                localIndex++;
            }
        } else if (logOperation instanceof UpdateOperation) {
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                int localIndex = skipFieldName();
                skipField();
                getNextBytesIntoTmp();
                ((UpdateOperation) logOperation).addData(localIndex, tmpBuffer);
            }
        } else {
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldName();
                skipField();
                skipNull();
            }
        }

        // skip '|' and `\n`
        nextIndex += 2;
        return logOperation;
    }

    public void compute() throws InterruptedException, ExecutionException {
        while (nextIndex < endIndex) {
            localOperations.add(scanOneRecord());
        }

        // wait for producing tasks
        LogOperation[] logOperations = localOperations.toArray(new LogOperation[0]);
        prevFuture.get();
        PipelinedComputation.blockingQueue.put(logOperations);
    }
}
