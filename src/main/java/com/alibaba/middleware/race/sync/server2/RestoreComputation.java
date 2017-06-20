package com.alibaba.middleware.race.sync.server2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public HashMap<LogOperation, LogOperation> recordMap = new HashMap<>();
    public HashSet<LogOperation> inRangeRecordSet = new HashSet<>();

    void compute(ArrayList<LogOperation> logOperations) {
        for (int i = 0; i < logOperations.size(); i++) {
            LogOperation logOperation = logOperations.get(i);
            if (logOperation instanceof DeleteOperation) {
                recordMap.remove(logOperation);
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.remove(logOperation);
                }
            } else if (logOperation instanceof InsertOperation) {
                recordMap.put(logOperation, logOperation);
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.add(logOperation);
                }
            } else {
                // update
                InsertOperation insertOperation = (InsertOperation) recordMap.get(logOperation);
                insertOperation.mergeUpdate((UpdateOperation) logOperation);

                if (logOperation instanceof UpdateKeyOperation) {
                    recordMap.remove(logOperation);
                    if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                        inRangeRecordSet.remove(logOperation);
                    }

                    insertOperation.changePK(((UpdateKeyOperation) logOperation).changedKey);
                    recordMap.put(insertOperation, insertOperation);
                    if (PipelinedComputation.isKeyInRange(insertOperation.relevantKey)) {
                        inRangeRecordSet.add(insertOperation);
                    }
                }
            }
        }

    }

    // used by master thread
    void parallelEvalAndSend(ExecutorService evalThreadPool) {
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
