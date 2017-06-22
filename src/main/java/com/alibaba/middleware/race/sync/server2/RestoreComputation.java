package com.alibaba.middleware.race.sync.server2;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.*;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {


    //    public static HashMap<LogOperation, LogOperation> recordMap = new HashMap<>();
    public static HashSet<LogOperation> inRangeRecordSet = new HashSet<>();
    public static Queue<Future<?>> futures = new LinkedList<>();

    static void submitJobs() {
        for (int i = 0; i < SLAVERS_NUM; i++) {
            final int finalI = i;
            futures.add(computationSlaverPools[i].submit(new Runnable() {
                @Override
                public void run() {
                    ArrayList<LogOperation> myOperations = logOperationsArr[finalI];
                    HashMap<LogOperation, LogOperation> myHashmap = recordMapArr[finalI];
                    for (int j = 0; j < myOperations.size(); j++) {
                        LogOperation logOperation = myOperations.get(j);
                        if (logOperation instanceof InsertOperation) {
                            myHashmap.put(logOperation, logOperation);
                        } else {
                            // update
                            InsertOperation insertOperation = (InsertOperation) recordMapArr[finalI].get(logOperation); //2
                            insertOperation.mergeAnother((UpdateOperation) logOperation); //3
                        }
                    }
                    // empty my tmp queue
                    myOperations.clear();
                }
            }));
        }
    }

    static void condWait() {
        while (!futures.isEmpty()) {
            try {
                futures.poll().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    static void compute(LogOperation[] logOperations) {
        for (int i = 0; i < logOperations.length; i++) {
            LogOperation logOperation = logOperations[i];
            if (logOperation instanceof DeleteOperation) {
                //recordMap.remove(logOperation);
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.remove(logOperation);
                }
            } else if (logOperation instanceof InsertOperation) {
//                recordMap.put(logOperation, logOperation); //1
                logOperationsArr[(int) (logOperation.relevantKey % SLAVERS_NUM)].add(logOperation);
                if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                    inRangeRecordSet.add(logOperation);
                }
            } else {
                // update

//                InsertOperation insertOperation = (InsertOperation) recordMap.get(logOperation); //2
//                insertOperation.mergeAnother((UpdateOperation) logOperation); //3
                logOperationsArr[(int) (logOperation.relevantKey % SLAVERS_NUM)].add(logOperation);
                if (logOperation instanceof UpdateKeyOperation) {
//                    recordMap.remove(logOperation);

                    submitJobs();
                    if (PipelinedComputation.isKeyInRange(logOperation.relevantKey)) {
                        inRangeRecordSet.remove(logOperation);
                    }

                    condWait();

                    InsertOperation insertOperation = (InsertOperation) recordMapArr[(int) (logOperation.relevantKey % SLAVERS_NUM)].get(logOperation);
                    insertOperation.changePK(((UpdateKeyOperation) logOperation).changedKey); //4
                    recordMapArr[(int) (insertOperation.relevantKey % SLAVERS_NUM)].put(insertOperation, insertOperation); //5

//                    InsertOperation insertOperation=new InsertOperation(((UpdateKeyOperation) logOperation).changedKey);
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
