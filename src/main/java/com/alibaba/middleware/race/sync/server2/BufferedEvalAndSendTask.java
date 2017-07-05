package com.alibaba.middleware.race.sync.server2;


import com.alibaba.middleware.race.sync.server2.operations.InsertOperation;
import com.alibaba.middleware.race.sync.server2.operations.LogOperation;
import com.alibaba.middleware.race.sync.server2.operations.NonDeleteOperation;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.findResultListener;

/**
 * Created by yche on 6/18/17.
 */
class BufferedEvalAndSendTask implements Runnable {
    private static int MAX_SIZE = 40000; // tuning it.................
    private LogOperation[] recordArr = new LogOperation[MAX_SIZE];
    private int nextIndex = 0;

    void addData(LogOperation line) {
        recordArr[nextIndex] = line;
        nextIndex++;
    }

    boolean isFull() {
        return nextIndex >= MAX_SIZE;
    }

    public int length() {
        return nextIndex;
    }

    public LogOperation get(int idx) {
        return recordArr[idx];
    }

    @Override
    public void run() {
        for (int i = 0; i < nextIndex; i++) {
            recordArr[i] = DatabaseRestore.getLogOperation(recordArr[i].relevantKey);
            String result = ((NonDeleteOperation) recordArr[i]).getOneLine();
            finalResultMap.put(recordArr[i].relevantKey, result);
            findResultListener.sendToClient(result);
        }
    }
}
