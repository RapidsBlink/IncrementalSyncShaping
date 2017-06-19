package com.alibaba.middleware.race.sync.server2;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 6/19/17.
 */
public class DeleteOperation extends LogOperation {
    public static ConcurrentLinkedQueue<DeleteOperation> freedPool = new ConcurrentLinkedQueue<>();
    private static ReentrantLock mallocLock = new ReentrantLock();

    private DeleteOperation() {
    }

    private static void extendQueue() {
        mallocLock.lock();
        if (freedPool.size() < 1024) {
            for (int i = 0; i < 1024; i++) {
                freedPool.add(new DeleteOperation());
            }
        }
        mallocLock.unlock();
    }

    public static DeleteOperation newDeleteOperation(long pk) {
        DeleteOperation borrow;
        while ((borrow = freedPool.poll()) == null) {
            extendQueue();
        }
        borrow.relevantKey = pk;
        return borrow;
    }

    @Override
    public void free() {
        freedPool.add(this);
    }
}
