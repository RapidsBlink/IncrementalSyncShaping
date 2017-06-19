package com.alibaba.middleware.race.sync.server2;

import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    private HashMap<LogOperation, LogOperation> recordMap = new HashMap<>();
    public TreeSet<LogOperation> inRangeRecordSet = new TreeSet<>();

    void compute(LogOperation logOperation) {
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
                insertOperation.changePK(((UpdateKeyOperation) logOperation).changedKey);
                recordMap.put(insertOperation, insertOperation);
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.remove(logOperation);
                }
                if (PipelinedComputation.isKeyInRange(insertOperation.relevantKey)) {
                    inRangeRecordSet.add(insertOperation);
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
