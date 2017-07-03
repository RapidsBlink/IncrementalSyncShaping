package com.alibaba.middleware.race.sync.server2;


import com.alibaba.middleware.race.sync.operations.RecordOperation;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.findResultListener;

/**
 * Created by yche on 6/18/17.
 */
class BufferedEvalAndSendTask implements Runnable {
    private static int MAX_SIZE = 40000; // tuning it.................
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
            String result = recordOperations[i].getOneLine(recordKeys[i]);
            finalResultMap.put(recordKeys[i], result);
            findResultListener.sendToClient(result);
        }
    }
}
