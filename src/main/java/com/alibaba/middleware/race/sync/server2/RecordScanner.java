package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.server2.operations.InsertOperation;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.Constants.*;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.CHUNK_SIZE;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.WORK_NUM;
import static com.alibaba.middleware.race.sync.server2.RecordField.fieldSkipLen;
import static com.alibaba.middleware.race.sync.server2.operations.InsertOperation.getIndexOfChineseChar;

/**
 * Created by yche on 6/18/17.
 * used for scan the byte arr of record string lines
 */
public class RecordScanner {
    // input
    private ByteBuffer mappedByteBuffer;
    private int endIndex;   // exclusive

    // intermediate states
    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(8);
    private int nextIndex; // start from startIndex

    private ByteBuffer byteBuffer = ByteBuffer.allocate(CHUNK_SIZE / WORK_NUM / 2);
    private Future<?> prevFuture;

    public RecordScanner() {
    }

    public void reuse(ByteBuffer mappedByteBuffer, int startIndex, int endIndex, Future<?> prevFuture) {
        this.mappedByteBuffer = mappedByteBuffer.asReadOnlyBuffer(); // get a view, with local position, limit
        this.nextIndex = startIndex;
        this.endIndex = endIndex;
        this.prevFuture = prevFuture;
    }

    public void reuse(ByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
        this.mappedByteBuffer = mappedByteBuffer.asReadOnlyBuffer();
        this.nextIndex = startIndex;
        this.endIndex = endIndex;
    }

    private void skipField(byte index) {
        switch (index) {
            case 0:
                nextIndex += 4;
                break;
            case 1:
                nextIndex += 4;
                if (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER)
                    nextIndex += 3;
                break;
            case 2:
                nextIndex += 4;
                break;
            default:
                nextIndex += 3;
                while (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER) {
                    nextIndex++;
                }
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
        nextIndex += RecordField.KEY_LEN + 3;
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

    private byte skipFieldName() {
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

    private void scanOneRecord() {
        // 1st: skip: mysql, ts, schema, table
        skipHeader();

        // 2nd: parse KeyOperation
        byte operation = mappedByteBuffer.get(nextIndex + 1);
        // skip one splitter and operation byte
        skipKey();

        if (operation == U_OPERATION) {
            // update
            long prevKey = getNextLong();
            long curKey = getNextLong();
            if (prevKey == curKey) {
                // update property
                byte localIndex = skipFieldName();
                skipField(localIndex);
                getNextBytesIntoTmp();
                switch (localIndex) {
                    case 0:
                        byteBuffer.put(U_FIRST_NAME);
                        byteBuffer.putLong(prevKey);
                        byteBuffer.put(getIndexOfChineseChar(tmpBuffer.array(), 0));
                        break;
                    case 1:
                        byteBuffer.put(U_LAST_NAME);
                        byteBuffer.putLong(prevKey);
                        byteBuffer.put(getIndexOfChineseChar(tmpBuffer.array(), 0));
                        if (tmpBuffer.limit() == 6)
                            byteBuffer.put(getIndexOfChineseChar(tmpBuffer.array(), 3));
                        else {
                            byte nullByte = -1;
                            byteBuffer.put(nullByte);
                        }
                        break;
                    case 2:
                        byteBuffer.put(U_SEX);
                        byteBuffer.putLong(prevKey);
                        byteBuffer.put(getIndexOfChineseChar(tmpBuffer.array(), 0));
                        break;
                    case 3:
                        short result = 0;
                        for (int i = 0; i < tmpBuffer.limit(); i++)
                            result = (short) ((10 * result) + (tmpBuffer.get(i) - '0'));

                        byteBuffer.put(U_SCORE);
                        byteBuffer.putLong(prevKey);
                        byteBuffer.putShort(result);
                        break;
                    case 4:
                        int resultInt = 0;
                        for (int i = 0; i < tmpBuffer.limit(); i++)
                            resultInt = ((10 * resultInt) + (tmpBuffer.get(i) - '0'));
                        byteBuffer.put(U_SCORE2);
                        byteBuffer.putLong(prevKey);
                        byteBuffer.putInt(resultInt);
                        break;
                    default:
                        if (Server.logger != null)
                            Server.logger.info("add data error");
                        System.err.println("add data error");
                }
            } else {
                // update key
                byteBuffer.put(U_PK);
                byteBuffer.putLong(prevKey);
                byteBuffer.putLong(curKey);
            }
        } else if (operation == I_OPERATION) {
            // insert: pre(null) -> cur
            byteBuffer.put(I_OP);
            skipNull();
            byteBuffer.putLong(getNextLong());

            byte localIndex = 0;
            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                skipFieldForInsert(localIndex);
                skipNull();
                getNextBytesIntoTmp();
                InsertOperation.addDataIntoByteBuffer(byteBuffer, localIndex, tmpBuffer);
                localIndex++;
            }
        } else {
            // delete: pre -> cur(null)
            byteBuffer.put(D_OP);
            byteBuffer.putLong(getNextLong());
            skipNull();

            while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
                byte localIndex = skipFieldName();
                skipField(localIndex);
                skipNull();
            }
        }

        // skip '|' and `\n`
        nextIndex += 2;
    }

    public void compute() {
        while (nextIndex < endIndex) {
            scanOneRecord();
        }
    }

    public void waitForSend() throws InterruptedException, ExecutionException {
        // wait for producing tasks
        byteBuffer.flip();
        ByteBuffer tmpByteBuffer = ByteBuffer.allocate(this.byteBuffer.limit());
        tmpByteBuffer.put(byteBuffer);
        tmpByteBuffer.flip();

//        System.out.print(byteBuffer.capacity() + "," + tmpByteBuffer.limit() + "\n");
        byteBuffer.clear();
        prevFuture.get();
//        System.out.println(tmpByteBuffer.limit());
        PipelinedComputation.blockingQueue.put(tmpByteBuffer);
    }
}
