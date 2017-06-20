package com.alibaba.middleware.race.sync.server2;

import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

import static com.alibaba.middleware.race.sync.Constants.D_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.I_OPERATION;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public static ConcurrentHashMap<Long, RecordOperation> currentDB = new ConcurrentHashMap<>();
    public static ConcurrentSkipListSet<Long> inRangeKeySet = new ConcurrentSkipListSet<>();

    // should be used in a single bsp chunk
    static void threadSafeEval(RecordOperation recordOperation) {
        if (recordOperation.filedValuePointers != null) {
            for (int i = 0; i < RecordField.FILED_NUM; i++) {
                if (recordOperation.filedValuePointers[i] instanceof FieldValueLazyEval) {
                    recordOperation.filedValuePointers[i] = ((FieldValueLazyEval) recordOperation.filedValuePointers[i]).eval();
                }
            }
        }
    }

    // should be used in a single bsp chunk
     static void threadSafeComputation(long key, RecordOperation recordOperation) {
        if (recordOperation.operationType == D_OPERATION) {
            if (currentDB.containsKey(key)) {
                currentDB.remove(key);
            }
            if (PipelinedComputation.isKeyInRange(key)) {
                inRangeKeySet.remove(key);
            }
        } else {
            // insert or update-property only operation
            currentDB.put(key, recordOperation);
            if (PipelinedComputation.isKeyInRange(key)) {
                inRangeKeySet.add(key);
            }
        }
    }

    // used by master thread
    static void parallelEvalAndSend(ExecutorService evalThreadPool) {
        BufferedEvalAndSendTask bufferedTask = new BufferedEvalAndSendTask();
        for (Long key : inRangeKeySet) {
            if (bufferedTask.isFull()) {
                evalThreadPool.execute(bufferedTask);
                bufferedTask = new BufferedEvalAndSendTask();
            }
            bufferedTask.addData(key, currentDB.get(key));
        }
        evalThreadPool.execute(bufferedTask);
    }
}
