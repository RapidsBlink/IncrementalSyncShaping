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
    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(64);
    private final ByteBuffer fieldNameBuffer = ByteBuffer.allocate(64);
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
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER) {
            nextIndex++;
        }
        while (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER) {
            nextIndex++;
        }
    }

    private void skipHeader() {
        nextIndex += 15;
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
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER)
            nextIndex++;

        byte tmpByte;
        long result = 0L;
        while ((tmpByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            nextIndex++;
            result = (10 * result) + (tmpByte - '0');
        }
        return result;
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
        // must be insert and update
        if (logOperation instanceof InsertOperation) {
            int localIndex = 0;
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldForInsert(localIndex);
                skipNull();
                byte[] nextBytes = getNextBytes();
                ((InsertOperation) logOperation).addValue(localIndex, nextBytes);
                localIndex++;
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
            if (prevFuture.isDone()) {
                if (!localOperations.isEmpty()) {
                    PipelinedComputation.blockingQueue.addAll(localOperations);
                } else {
                    PipelinedComputation.blockingQueue.add(scanOneRecord());
                }

            } else {
                localOperations.add(scanOneRecord());
            }
        }

        // wait for producing tasks
        try {
            prevFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        if (!localOperations.isEmpty()) {
            PipelinedComputation.blockingQueue.addAll(localOperations);
        }
    }
}
