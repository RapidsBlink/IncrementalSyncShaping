package com.alibaba.middleware.race.sync.server2;


/**
 * Created by yche on 6/20/17.
 */
public class BufferedEvalTask implements Runnable {
    private static int MAX_SIZE = 4000; // tuning it.................
    private RecordOperation[] recordOperations = new RecordOperation[MAX_SIZE];
    private int nextIndex = 0;

    void addData(RecordOperation recordOperation) {
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
            RestoreComputation.threadSafeEval(recordOperations[i]);
        }
    }
}
