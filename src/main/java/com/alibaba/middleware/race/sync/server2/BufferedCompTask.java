package com.alibaba.middleware.race.sync.server2;


import com.alibaba.middleware.race.sync.operations.RecordOperation;

/**
 * Created by yche on 6/20/17.
 */
public class BufferedCompTask implements Runnable {
    private static int MAX_SIZE = 4000; // tuning it.................
    private long[] recordKeys = new long[MAX_SIZE];
    private RecordOperation[] recordOperations = new RecordOperation[MAX_SIZE];
    private int nextIndex = 0;

    void addData(long line, RecordOperation recordOperation) {
        recordKeys[nextIndex] = line;
        recordOperations[nextIndex] = recordOperation;
        nextIndex++;
    }

    boolean isFull() {
        return nextIndex >= MAX_SIZE;
    }

    public int length() {
        return nextIndex;
    }

    @Override
    public void run() {
        for (int i = 0; i < nextIndex; i++) {
            RestoreComputation.threadSafeComputation(recordKeys[i],recordOperations[i]);
        }
    }
}
