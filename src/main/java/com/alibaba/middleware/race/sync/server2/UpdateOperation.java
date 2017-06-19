package com.alibaba.middleware.race.sync.server2;


import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateOperation extends LogOperation {
    public static ConcurrentLinkedQueue<UpdateOperation> freedPool = new ConcurrentLinkedQueue<>();
    private static ReentrantLock mallocLock = new ReentrantLock();

    public byte[][] valueArr;

    public void addValue(ByteBuffer keyBytes, byte[] bytes) {
        valueArr[RecordField.fieldIndexMap.get(keyBytes)] = bytes;
    }

    public boolean isKeyChanged() {
        return this instanceof UpdateKeyOperation;
    }

    public UpdateOperation(){
        valueArr=new byte[RecordField.FILED_NUM][];
    }

    private static void extendQueue() {
        mallocLock.lock();
        if (freedPool.size() < 1024) {
            for (int i = 0; i < 1024; i++) {
                freedPool.add(new UpdateOperation());
            }
        }
        mallocLock.unlock();
    }

    public static UpdateOperation newUpdateOperation(long prevKey) {
        UpdateOperation borrow;
        while ((borrow = freedPool.poll()) == null) {
            extendQueue();
        }
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            borrow.valueArr[i] = null;
        }
        borrow.relevantKey = prevKey;
        return borrow;
    }

    @Override
    public void free() {
        freedPool.add(this);
    }
}
