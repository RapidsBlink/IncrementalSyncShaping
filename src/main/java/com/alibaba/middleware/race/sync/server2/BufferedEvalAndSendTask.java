package com.alibaba.middleware.race.sync.server2;


import com.alibaba.middleware.race.sync.server2.operations.InsertOperation;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;

/**
 * Created by yche on 6/18/17.
 */
class BufferedEvalAndSendTask implements Runnable {
    private static int MAX_SIZE = 40000; // tuning it.................
    private InsertOperation[] recordArr = new InsertOperation[MAX_SIZE];
    private int nextIndex = 0;

    void addData(InsertOperation line) {
        recordArr[nextIndex] = line;
        nextIndex++;
    }

    boolean isFull() {
        return nextIndex >= MAX_SIZE;
    }

    public int length() {
        return nextIndex;
    }

    public InsertOperation get(int idx) {
        return recordArr[idx];
    }

    @Override
    public void run() {
        for (int i = 0; i < nextIndex; i++) {
            finalResultMap.put(recordArr[i].relevantKey, recordArr[i].getOneLineBytesEfficient());
        }
    }
}
