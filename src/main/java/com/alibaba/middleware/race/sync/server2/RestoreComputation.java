package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.server2.operations.*;
import gnu.trove.set.hash.THashSet;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.EVAL_WORKER_NUM;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.finalResultMap;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    public static YcheLongObjectHashMap recordMap = new YcheLongObjectHashMap(20 * 1024 * 1024);
    public static THashSet<InsertOperation> inRangeRecordSet = new THashSet<>(2 * 1024 * 1024);
    public static InsertOperation tmp = new InsertOperation(-1);

    // byteBuffer should be flipped first
    static void compute(ByteBuffer byteBuffer) {
        while (byteBuffer.hasRemaining()) {
            byte op = byteBuffer.get();
            long prePk = byteBuffer.getLong();

            switch (op) {
                case Constants.D_OP:
                    if (PipelinedComputation.isKeyInRange(prePk)) {
                        tmp.relevantKey = prePk;
                        inRangeRecordSet.remove(tmp);
                    }
                    break;
                case Constants.I_OP:
                    InsertOperation newInsertion = new InsertOperation(prePk);
                    newInsertion.firstNameIndex = (byteBuffer.get());
                    newInsertion.lastNameFirstIndex = byteBuffer.get();
                    newInsertion.lastNameSecondIndex = byteBuffer.get();
                    newInsertion.sexIndex = byteBuffer.get();
                    newInsertion.score = byteBuffer.getShort();
                    if (RecordField.FILED_NUM > 4)
                        newInsertion.score2 = byteBuffer.getInt();
                    recordMap.put(newInsertion); //1
                    if (PipelinedComputation.isKeyInRange(prePk)) {
                        inRangeRecordSet.add(newInsertion);
                    }
                    break;
                default:
                    InsertOperation insertOperation = recordMap.get(prePk); //2
                    switch (op) {
                        case Constants.U_SCORE:
                            insertOperation.score = byteBuffer.getShort();
                            break;
                        case Constants.U_SCORE2:
                            insertOperation.score2 = byteBuffer.getInt();
                            break;
                        case Constants.U_FIRST_NAME:
                            insertOperation.firstNameIndex = byteBuffer.get();
                            break;
                        case Constants.U_LAST_NAME:
                            insertOperation.lastNameFirstIndex = byteBuffer.get();
                            insertOperation.lastNameSecondIndex = byteBuffer.get();
                            break;
                        case Constants.U_PK:
                            // update pk
                            if (PipelinedComputation.isKeyInRange(prePk)) {
                                inRangeRecordSet.remove(insertOperation);
                            }

                            insertOperation.changePK(byteBuffer.getLong()); //4
                            recordMap.put(insertOperation); //5

                            if (PipelinedComputation.isKeyInRange(insertOperation.relevantKey)) {
                                inRangeRecordSet.add(insertOperation);
                            }
                            break;
                        case Constants.U_SEX:
                            insertOperation.sexIndex = byteBuffer.get();
                            break;
                    }
            }
        }
    }

    private static class EvalTask implements Runnable {
        int start;
        int end;
        InsertOperation[] logOperations;

        EvalTask(int start, int end, InsertOperation[] logOperations) {
            this.start = start;
            this.end = end;
            this.logOperations = logOperations;
        }

        @Override
        public void run() {
            for (int i = start; i < end; i++) {
                InsertOperation insertOperation = logOperations[i];
                finalResultMap.put(insertOperation.relevantKey, insertOperation.getOneLineBytesEfficient());
            }
        }

    }

    // used by master thread
    static void parallelEvalAndSend(ExecutorService evalThreadPool) {
        InsertOperation[] insertOperations = inRangeRecordSet.toArray(new InsertOperation[0]);
        int avgTask = insertOperations.length / EVAL_WORKER_NUM;
        for (int i = 0; i < insertOperations.length; i += avgTask) {
            evalThreadPool.execute(new EvalTask(i, Math.min(i + avgTask, insertOperations.length), insertOperations));
        }
    }
}
