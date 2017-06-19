package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation extends LogOperation {
    public static ConcurrentLinkedQueue<InsertOperation> freedPool = new ConcurrentLinkedQueue<>();
    private static ReentrantLock mallocLock = new ReentrantLock();

    public byte[][] valueArr;

    private InsertOperation() {
        valueArr = new byte[RecordField.FILED_NUM][];
    }

    public void addValue(ByteBuffer keyBytes, byte[] bytes) {
        valueArr[RecordField.fieldIndexMap.get(keyBytes)] = bytes;
    }

    public void changePK(long newPk) {
        this.relevantKey = newPk;
    }

    public void mergeUpdate(UpdateOperation updateOperation) {
        if (updateOperation.valueArr != null) {
            for (int i = 0; i < RecordField.FILED_NUM; i++) {
                if (updateOperation.valueArr[i] != null) {
                    valueArr[i] = updateOperation.valueArr[i];
                }
            }
        }
    }

    String getOneLine() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(relevantKey).append('\t');
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            stringBuilder.append(new String(valueArr[i])).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    private static void extendQueue() {
        mallocLock.lock();
        if (freedPool.size() < 1024) {
            for (int i = 0; i < 1024 * 512; i++) {
                freedPool.add(new InsertOperation());
            }
        }
        mallocLock.unlock();
    }

    public static InsertOperation newInsertOperation(long prevKey) {
        InsertOperation borrow;
        while ((borrow = freedPool.poll()) == null) {
            extendQueue();
        }
        borrow.relevantKey = prevKey;
        return borrow;
    }

    @Override
    public void free() {
        freedPool.add(this);
    }
}
