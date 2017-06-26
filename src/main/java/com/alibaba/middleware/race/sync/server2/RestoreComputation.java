package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.*;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.THashSet;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.EVAL_WORKER_NUM;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public static YcheHashMap recordMap;
    public static THashSet<LogOperation> inRangeRecordSet;
    public static TLongIntHashMap keyIntMap = new TLongIntHashMap();

    static void compute(LogOperation[] logOperations) {
        for (int i = 0; i < logOperations.length; i++) {
            LogOperation logOperation = logOperations[i];

            if (logOperation instanceof UpdateOperation) {
                InsertOperation insertOperation = (InsertOperation) recordMap.get(logOperation); //2
                insertOperation.mergeAnother((UpdateOperation) logOperation); //3
            } else if (logOperation instanceof UpdateKeyOperation) {
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.remove(logOperation);
                }
                recordMap.put(new InsertOperation(((UpdateKeyOperation) logOperation).changedKey)); //5

                if (PipelinedComputation.isKeyInRange((((UpdateKeyOperation) logOperation).changedKey))) {
                    inRangeRecordSet.add(new InsertOperation(((UpdateKeyOperation) logOperation).changedKey));
                }
            } else if (logOperation instanceof InsertOperation) {
                recordMap.put(logOperation); //1
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.add(logOperation);
                }
            } else {
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.remove(logOperation);
                }
            }
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
