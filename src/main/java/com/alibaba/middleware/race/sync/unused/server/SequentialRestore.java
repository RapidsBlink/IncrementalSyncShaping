package com.alibaba.middleware.race.sync.unused.server;

import com.alibaba.middleware.race.sync.Server;

import java.util.Arrays;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;
import static com.alibaba.middleware.race.sync.unused.server.ServerPipelinedComputation.*;

/**
 * Created by yche on 6/8/17.
 */
public class SequentialRestore {
    // data
    private RecordLazyEval recordLazyEval;

    private void initFieldListIfFirstTime() {
        if (filedList.size() == 0) {
            RecordFields record = new RecordFields(recordLazyEval.recordStr);
            filedList = record.colOrder;
            if (Server.logger != null)
                Server.logger.info(Arrays.toString(record.colOrder.toArray()));
        }
    }

    private void actForDelete() {
        deadKeys.add(recordLazyEval.prevPKVal);
    }

    private void addTaskToPool(RecordUpdate recordUpdate, RecordLazyEval recordLazyEval) {
        int poolIndex = (int) (recordUpdate.lastKey % EVAL_UPDATE_WORKER_NUM);
        if (evalUpdateApplyTasks[poolIndex] == null)
            evalUpdateApplyTasks[poolIndex] = new EvalUpdateTaskBuffer();

        if (evalUpdateApplyTasks[poolIndex].isFull()) {
            evalUpdateApplyPools[poolIndex].execute(new EvalUpdateApplyTask(evalUpdateApplyTasks[poolIndex]));
            evalUpdateApplyTasks[poolIndex] = new EvalUpdateTaskBuffer();
        }
        evalUpdateApplyTasks[poolIndex].addData(recordUpdate, recordLazyEval);
    }

    void flushTasksToPool() {
        for (int i = 0; i < EVAL_UPDATE_WORKER_NUM; i++) {
            if (evalUpdateApplyTasks[i] != null)
                evalUpdateApplyPools[i].execute(new EvalUpdateApplyTask(evalUpdateApplyTasks[i]));
            evalUpdateApplyTasks[i] = null;
        }
    }

    private void actForInsert() {
        initFieldListIfFirstTime();

        if (deadKeys.contains(recordLazyEval.curPKVal)) {
            // insert-delete
            deadKeys.remove(recordLazyEval.curPKVal);
        } else if (outOfRangeActiveKeys.contains(recordLazyEval.curPKVal)) {
            // insert-update out-of-range
            outOfRangeActiveKeys.remove(recordLazyEval.curPKVal);
        } else if (inRangeActiveKeys.containsKey(recordLazyEval.curPKVal)) {
            // insert-update in-range
            RecordUpdate prevUpdate = inRangeActiveKeys.get(recordLazyEval.curPKVal);
            addTaskToPool(prevUpdate, recordLazyEval);

            inRangeActiveKeys.remove(recordLazyEval.curPKVal);

            // write recordStr to tree map
        } else {
            // first-time appearing
            if (isKeyInRange(recordLazyEval.curPKVal)) {
                // write recordStr to skip list
                RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
                addTaskToPool(recordUpdate, recordLazyEval);
            }
            // else do nothing
        }
    }

    private void actForUpdate() {
        if (deadKeys.contains(recordLazyEval.curPKVal)) {
            // update-delete
            if (recordLazyEval.isPKUpdate()) {
                deadKeys.remove(recordLazyEval.curPKVal);
                deadKeys.add(recordLazyEval.prevPKVal);
            }
        } else if (outOfRangeActiveKeys.contains(recordLazyEval.curPKVal)) {
            // update-update out-of-range
            if (recordLazyEval.isPKUpdate()) {
                outOfRangeActiveKeys.remove(recordLazyEval.curPKVal);
                outOfRangeActiveKeys.add(recordLazyEval.prevPKVal);
            }
        } else if (inRangeActiveKeys.containsKey(recordLazyEval.curPKVal)) {
            // update-update in-range
            RecordUpdate prevUpdate = inRangeActiveKeys.get(recordLazyEval.curPKVal);
            addTaskToPool(prevUpdate, recordLazyEval);
            if (recordLazyEval.isPKUpdate()) {
                inRangeActiveKeys.remove(recordLazyEval.curPKVal);
                inRangeActiveKeys.put(recordLazyEval.prevPKVal, prevUpdate);
            }
        } else {
            // first-time appearing
            if (isKeyInRange(recordLazyEval.curPKVal)) {
                // add to inRangeActiveKeys
                RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
                addTaskToPool(recordUpdate, recordLazyEval);

                inRangeActiveKeys.put(recordLazyEval.prevPKVal, recordUpdate);
            } else {
                // add to outOfRangeActiveKeys
                outOfRangeActiveKeys.add(recordLazyEval.prevPKVal);
            }
        }
    }

    public void compute(RecordLazyEval recordLazyEval) {
        this.recordLazyEval = recordLazyEval;
        //if (recordLazyEval.isSchemaTableValid()) {
        if (recordLazyEval.operationType == DELETE_OPERATION) {
            actForDelete();
        } else if (recordLazyEval.operationType == INSERT_OPERATION) {
            actForInsert();
        } else {
            actForUpdate();
        }
        //}
    }
}
