package com.alibaba.middleware.race.sync.server;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;
import static com.alibaba.middleware.race.sync.server.ServerPipelinedComputation.*;

/**
 * Created by yche on 6/8/17.
 */
public class SequentialRestore {
    // data
    private RecordLazyEval recordLazyEval;
    private String result = null;

    private void initFieldListIfFirstTime() {
        if (filedList.size() == 0) {
            RecordFields record = new RecordFields(recordLazyEval.recordStr);
            filedList = record.colOrder;
        }
    }

    private void actForDelete() {
        deadKeys.add(recordLazyEval.prevPKVal);
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
            prevUpdate.addEntriesIfNotThere(recordLazyEval);
            inRangeActiveKeys.remove(recordLazyEval.curPKVal);

            // write recordStr to tree map
            inRangeRecord.put(prevUpdate.lastKey, prevUpdate.toOneLineString(filedList));
            result = prevUpdate.toOneLineString(filedList);
        } else {
            // first-time appearing
            if (isKeyInRange(recordLazyEval.curPKVal)) {
                // write recordStr to skip list
                RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
                recordUpdate.addEntriesIfNotThere(recordLazyEval);
                inRangeRecord.put(recordUpdate.lastKey, recordUpdate.toOneLineString(filedList));
                result = recordUpdate.toOneLineString(filedList);
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
            prevUpdate.addEntriesIfNotThere(recordLazyEval);
            if (recordLazyEval.isPKUpdate()) {
                inRangeActiveKeys.remove(recordLazyEval.curPKVal);
                inRangeActiveKeys.put(recordLazyEval.prevPKVal, prevUpdate);
            }
        } else {
            // first-time appearing
            if (isKeyInRange(recordLazyEval.curPKVal)) {
                // add to inRangeActiveKeys
                RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
                recordUpdate.addEntriesIfNotThere(recordLazyEval);
                inRangeActiveKeys.put(recordLazyEval.prevPKVal, recordUpdate);
            } else {
                // add to outOfRangeActiveKeys
                outOfRangeActiveKeys.add(recordLazyEval.prevPKVal);
            }
        }
    }

    public String compute(RecordLazyEval recordLazyEval) {
        String ret = null;
        this.recordLazyEval = recordLazyEval;
        if (recordLazyEval.isSchemaTableValid()) {
            if (recordLazyEval.operationType == DELETE_OPERATION) {
                actForDelete();
            } else if (recordLazyEval.operationType == INSERT_OPERATION) {
                actForInsert();
                ret = result;
                result = null;
            } else {
                actForUpdate();
            }
        }
        return ret;
    }
}
