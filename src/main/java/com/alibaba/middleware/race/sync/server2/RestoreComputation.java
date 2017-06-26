package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.*;
import gnu.trove.set.hash.THashSet;

import java.util.concurrent.ExecutorService;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.EVAL_WORKER_NUM;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public static YcheHashMap recordMap = new YcheHashMap(24 * 1024 * 1024);

    public static THashSet<LogOperation> inRangeRecordSet = new THashSet<>(4 * 1024 * 1024);

    static void compute(LogOperation[] logOperations) {
        for (int i = 0; i < logOperations.length; i++) {
            logOperations[i].act();
        }
    }

    private static class EvalTask implements Runnable {
        int start;
        int end;
        LogOperation[] logOperations;

        EvalTask(int start, int end, LogOperation[] logOperations) {
            this.start = start;
            this.end = end;
            this.logOperations = logOperations;
        }

        @Override
        public void run() {
            for (int i = start; i < end; i++) {
                InsertOperation insertOperation = (InsertOperation) logOperations[i];
                finalResultMap.put(insertOperation.relevantKey, insertOperation.getOneLineBytesEfficient());
            }
        }
    }

    // used by master thread
    static void parallelEvalAndSend(ExecutorService evalThreadPool) {
        LogOperation[] insertOperations = inRangeRecordSet.toArray(new LogOperation[0]);
        int avgTask = insertOperations.length / EVAL_WORKER_NUM;
        for (int i = 0; i < insertOperations.length; i += avgTask) {
            evalThreadPool.execute(new EvalTask(i, Math.min(i + avgTask, insertOperations.length), insertOperations));
        }
    }
}
