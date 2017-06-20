package com.alibaba.middleware.race.sync.server2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

import static com.alibaba.middleware.race.sync.Constants.D_OPERATION;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public static ConcurrentHashMap<Long, RecordOperation> currentDB = new ConcurrentHashMap<>();
    public static ConcurrentSkipListSet<Long> inRangeKeySet = new ConcurrentSkipListSet<>();
    private static Queue<Future<?>> waitQueue = new LinkedList<>();

    // should be used in a single bsp chunk
    public static void threadSafeEval(RecordOperation recordOperation) {
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

    static void parallelEval(ExecutorService computationPool, HashMap<Long, RecordOperation> recordOperationHashMap) {
        BufferedEvalTask bufferedTask = new BufferedEvalTask();
        for (RecordOperation recordOperation : recordOperationHashMap.values()) {
            if (bufferedTask.isFull()) {
                waitQueue.add(computationPool.submit(bufferedTask));
                bufferedTask = new BufferedEvalTask();
            }
            bufferedTask.addData(recordOperation);
        }
        waitQueue.add(computationPool.submit(bufferedTask));
        while (!waitQueue.isEmpty()) {
            try {
                waitQueue.poll().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    static void parallelComp(ExecutorService computationPool, HashMap<Long, RecordOperation> recordOperationHashMap) {
        BufferedCompTask bufferedTask = new BufferedCompTask();
        for (Map.Entry<Long, RecordOperation> entry : recordOperationHashMap.entrySet()) {
            if (bufferedTask.isFull()) {
                waitQueue.add(computationPool.submit(bufferedTask));
                bufferedTask = new BufferedCompTask();
            }
            bufferedTask.addData(entry.getKey(), entry.getValue());
        }
        waitQueue.add(computationPool.submit(bufferedTask));
        while (!waitQueue.isEmpty()) {
            try {
                waitQueue.poll().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
