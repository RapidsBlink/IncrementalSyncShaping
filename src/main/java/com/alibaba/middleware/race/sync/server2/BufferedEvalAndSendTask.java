package com.alibaba.middleware.race.sync.server2;


import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.findResultListener;

/**
 * Created by yche on 6/18/17.
 */
class BufferedEvalAndSendTask implements Runnable {
    private static int MAX_SIZE = 40000; // tuning it.................
    private RecordObject[] recordArr = new RecordObject[MAX_SIZE];
    private int nextIndex = 0;

    void addData(RecordObject line) {
        recordArr[nextIndex] = line;
        nextIndex++;
    }

    boolean isFull() {
        return nextIndex >= MAX_SIZE;
    }

    public int length() {
        return nextIndex;
    }

    public RecordObject get(int idx) {
        return recordArr[idx];
    }

    @Override
    public void run() {
        for (int i = 0; i < nextIndex; i++) {
            String result = recordArr[nextIndex].getOneLine();
            finalResultMap.put(recordArr[nextIndex].key, result);
            findResultListener.sendToClient(result);
        }
    }
}
