package com.alibaba.middleware.race.sync.play;

import com.alibaba.middleware.race.sync.server.RecordLazyEval;
import com.alibaba.middleware.race.sync.server.RecordUpdate;

import java.util.AbstractMap;
import java.util.ArrayList;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;
import static com.alibaba.middleware.race.sync.play.GlobalComputation.*;

/**
 * Created by yche on 6/8/17.
 */
public class SequentialImpl {
    // data
    private final ArrayList<String> fileChunk;
    private final int upperIdx;
    private final int lowerIdx;
    private int i;

    SequentialImpl(ArrayList<String> fileChunk, int upperIdx, int lowerIdx) {
        this.upperIdx = upperIdx;
        this.lowerIdx = lowerIdx;
        this.fileChunk = fileChunk;
    }

    private RecordLazyEval recordLazyEval;
    private StringBuilder stringBuilder = new StringBuilder();

    private void updateOtherFieldContents(RecordUpdate recordUpdate) {
        // update contents if possible
        while (recordLazyEval.hasNext()) {
            AbstractMap.SimpleEntry<String, Object> entry = recordLazyEval.next();
            recordUpdate.addEntryIfNotThere(entry.getKey(), entry.getValue());
        }
    }

    private void actForDelete() {
        deadKeys.add(recordLazyEval.prevPKVal);

    }

    private void actForInsert() {
        if (filedList.size() == 0) {
            Record record = new Record(fileChunk.get(i), true);
            filedList = record.colOrder;
        }

        if (deadKeys.contains(recordLazyEval.curPKVal)) {
            // insert-delete
            deadKeys.remove(recordLazyEval.curPKVal);
        } else if (outOfRangeActiveKeys.contains(recordLazyEval.curPKVal)) {
            // insert-update out-of-range
            outOfRangeActiveKeys.remove(recordLazyEval.curPKVal);
        } else if (inRangeActiveKeys.containsKey(recordLazyEval.curPKVal)) {
            // insert-update in-range
            RecordUpdate prevUpdate = inRangeActiveKeys.get(recordLazyEval.curPKVal);
            updateOtherFieldContents(prevUpdate);
            inRangeActiveKeys.remove(recordLazyEval.curPKVal);

            // write string to tree map
            inRangeRecord.put(prevUpdate.lastKey, prevUpdate.toOneLineString(filedList));
        } else {
            // first-time appearing
            if (isKeyInRange(recordLazyEval.curPKVal)) {
                // write string to skip list
                RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
                updateOtherFieldContents(recordUpdate);
                inRangeRecord.put(recordUpdate.lastKey, recordUpdate.toOneLineString(filedList));
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
            updateOtherFieldContents(prevUpdate);
            if (recordLazyEval.isPKUpdate()) {
                inRangeActiveKeys.remove(recordLazyEval.curPKVal);
                inRangeActiveKeys.put(recordLazyEval.prevPKVal, prevUpdate);
            }
        } else {
            // first-time appearing
            if (isKeyInRange(recordLazyEval.curPKVal)) {
                // add to inRangeActiveKeys
                RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
                updateOtherFieldContents(recordUpdate);
                inRangeActiveKeys.put(recordLazyEval.prevPKVal, recordUpdate);
            } else {
                // add to outOfRangeActiveKeys
                outOfRangeActiveKeys.add(recordLazyEval.prevPKVal);
            }
        }
    }

    public void compute() {
        for (i = upperIdx; i > lowerIdx; i--) {
            recordLazyEval = new RecordLazyEval(fileChunk.get(i), stringBuilder);
            if (recordLazyEval.operationType == DELETE_OPERATION) {
                actForDelete();
            } else if (recordLazyEval.operationType == INSERT_OPERATION) {
                actForInsert();
            } else {
                actForUpdate();
            }
        }
    }
}
