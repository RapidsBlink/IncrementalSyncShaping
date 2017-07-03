package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.*;
import gnu.trove.set.hash.THashSet;

import java.util.HashSet;
import java.util.concurrent.ExecutorService;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public static YcheHashMap recordMap = new YcheHashMap(20 * 1024 * 1024);

    public static THashSet<LogOperation> inRangeRecordSet = new THashSet<>(4 * 1024 * 1024);

    static void compute(LogOperation[] logOperations) {
        for (int i = 0; i < logOperations.length; i++) {
            logOperations[i].act();
        }
    }

    // used by master thread
    static void parallelEvalAndSend(ExecutorService evalThreadPool) {
        BufferedEvalAndSendTask bufferedTask = new BufferedEvalAndSendTask();
        for (LogOperation logOperation : inRangeRecordSet) {
            if (bufferedTask.isFull()) {
                evalThreadPool.execute(bufferedTask);
                bufferedTask = new BufferedEvalAndSendTask();
            }
            bufferedTask.addData((InsertOperation) logOperation);
        }
        evalThreadPool.execute(bufferedTask);
    }
}
