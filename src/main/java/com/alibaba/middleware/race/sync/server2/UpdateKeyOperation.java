package com.alibaba.middleware.race.sync.server2;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateKeyOperation extends UpdateOperation {
    public static ConcurrentLinkedQueue<UpdateKeyOperation> freedPool = new ConcurrentLinkedQueue<>();
    private static ReentrantLock mallocLock = new ReentrantLock();
    public long changedKey;

    private static void extendQueue() {
        mallocLock.lock();
        if (freedPool.size() < 1024) {
            for (int i = 0; i < 1024; i++) {
                freedPool.add(new UpdateKeyOperation());
            }
        }
        mallocLock.unlock();
    }

    private UpdateKeyOperation(){
        valueArr=new byte[RecordField.FILED_NUM][];
    }

//    public UpdateKeyOperation(long prevKey, long changedKey) {
//        super(prevKey);
//        this.changedKey = changedKey;
//    }

    public static UpdateKeyOperation newUpdateKeyOperation(long prevKey, long changedKey) {
        UpdateKeyOperation borrow;
        while ((borrow = freedPool.poll()) == null) {
            extendQueue();
        }
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            borrow.valueArr[i] = null;
        }
        borrow.relevantKey = prevKey;
        borrow.changedKey = changedKey;
        return borrow;
    }

    @Override
    public void free() {
        freedPool.add(this);
    }
}
