package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;

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
    private final TransformComputation transformComputation;

    public RecordScanner(ByteBuffer mappedByteBuffer, int startIndex, int endIndex, TransformComputation transformComputation) {
        this.mappedByteBuffer = mappedByteBuffer.asReadOnlyBuffer(); // get a view, with local position, limit
        this.nextIndex = startIndex;
        this.endIndex = endIndex;
        this.transformComputation = transformComputation;
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
        long result = 0l;
        while ((tmpByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            nextIndex++;
            result = (10 * result) + (tmpByte - '0');
        }
        return result;
    }

    private void fetchFieldName() {
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

    private void skipNull() {
        nextIndex += 5;
    }

    private void scanOneRecord() {
        // 1st: skip: mysql, ts, schema, table
        for (int i = 0; i < 4; i++) {
            skipField();
//            System.out.println(new String(getNextBytes()));
        }

        // 2nd: parse KeyOperation
        byte operation = mappedByteBuffer.get(nextIndex + 1);
        RecordOperation logOperation;
        // skip one splitter and operation byte
        nextIndex += 2;
        skipField();
        if (operation == I_OPERATION) {
            // insert: pre(null) -> cur
            skipNull();
            logOperation = transformComputation.insertPk(getNextLong());
            int localIndex = 0;
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldForInsert(localIndex);
                skipNull();
                byte[] nextBytes = getNextBytes();
                transformComputation.updateProperty(logOperation, localIndex, nextBytes);
                localIndex++;
            }
        } else if (operation == D_OPERATION) {
            // delete: pre -> cur(null)
            transformComputation.deletePk(getNextLong());
            skipNull();
            // skip next several fields
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                nextIndex++;
            }
        } else {
            // update
            long prevKey = getNextLong();
            long curKey = getNextLong();
            logOperation = transformComputation.updatePk(prevKey);

            // update properties
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                fetchFieldName();
                skipField();
                byte[] nextBytes = getNextBytes();
                int index = RecordField.fieldIndexMap.get(fieldNameBuffer);
                transformComputation.updateProperty(logOperation, index, nextBytes);
            }

            if (prevKey != curKey) {
                // update-key
                transformComputation.updatePkTransferProperties(prevKey, curKey);
            }
        }
        nextIndex += 2;
    }

    public void compute() {
        while (nextIndex < endIndex) {
            scanOneRecord();
        }
    }
}
