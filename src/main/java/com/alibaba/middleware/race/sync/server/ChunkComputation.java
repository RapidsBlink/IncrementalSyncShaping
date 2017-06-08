package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.play.Record;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.UPDATE_OPERATION;

/**
 * Created by yche on 6/7/17.
 * used by a single thread
 */
public class ChunkComputation {
    // observation1: at the beginning of this chunk, there is no duplicate primary keys
    // observation2: at the end of this chunk, there is no duplicate primary keys
    private Map<Long, RecordUpdate> activeKeys = new HashMap<>();
    private Map<Long, RecordUpdate> deadKeys = new HashMap<>();
    private ArrayList<RecordUpdate> insertOnlyUpdates = new ArrayList<>();

    private RecordLazyEval recordLazyEval;
    private StringBuilder stringBuilder = new StringBuilder();

    private void updateOtherFieldContents(RecordUpdate recordUpdate) {
        // update contents if possible
        while (recordLazyEval.hasNext()) {
            AbstractMap.SimpleEntry<String, Object> entry = recordLazyEval.next();
            recordUpdate.addEntryIfNotThere(entry.getKey(), entry.getValue());
        }
    }

    private void actForDeleteOperation() {
        // ?-delete mode, it is last operation for this record
        RecordUpdate recordUpdate = new RecordUpdate(recordLazyEval);
        deadKeys.put(recordUpdate.firstKey, recordUpdate);
    }

    private void actForInsertOperation() {
        if (deadKeys.containsKey(recordLazyEval.curPKVal)) {
            // insert-delete mode, prune it
            deadKeys.remove(recordLazyEval.curPKVal);
        } else {
            RecordUpdate recordUpdate = activeKeys.get(recordLazyEval.curPKVal);
            if (recordUpdate == null) {
                // insert-insert mode
                recordUpdate = new RecordUpdate(recordLazyEval);
                insertOnlyUpdates.add(recordUpdate);
            }
            // else: insert-update mode, do nothing with active keys
            recordUpdate.isFirstInsertInChunk = true;
            updateOtherFieldContents(recordUpdate);
        }
    }

    private void actForUpdateOperation() {
        RecordUpdate recordUpdate = deadKeys.get(recordLazyEval.curPKVal);
        if (recordUpdate != null) {
            // update-delete mode
            if (recordLazyEval.isPKUpdate()) {
                // in dead keys: remove previous key, add new key
                deadKeys.remove(recordUpdate.firstKey);
                recordUpdate.setFirstKey(recordLazyEval.prevPKVal);
                deadKeys.put(recordUpdate.firstKey, recordUpdate);
            }
        } else {
            recordUpdate = activeKeys.get(recordLazyEval.curPKVal);
            if (recordUpdate != null) {
                // update-update mode
                if (recordLazyEval.isPKUpdate()) {
                    // in active keys: remove previous key, add new key
                    activeKeys.remove(recordUpdate.firstKey);
                    recordUpdate.setFirstKey(recordLazyEval.prevPKVal);
                    activeKeys.put(recordUpdate.firstKey, recordUpdate);
                }
            } else {
                // ?-update mode, it is the last operation for this record
                recordUpdate = new RecordUpdate(recordLazyEval);
                activeKeys.put(recordUpdate.firstKey, recordUpdate);
            }

            // update contents if possible
            updateOtherFieldContents(recordUpdate);
        }
    }

    // used for each chunk computation
    public ArrayList<RecordUpdate> compute(ArrayList<String> fileChunk, int upper_idx, int lower_idx) {
        activeKeys.clear();
        deadKeys.clear();
        insertOnlyUpdates.clear();

        for (int i = upper_idx; i > lower_idx; i--) {
            recordLazyEval = new RecordLazyEval(fileChunk.get(i), stringBuilder);

            if (recordLazyEval.operationType == DELETE_OPERATION) {
                actForDeleteOperation();
            } else if (recordLazyEval.operationType == INSERT_OPERATION) {
                actForInsertOperation();
            } else if (recordLazyEval.operationType == UPDATE_OPERATION) {
                actForUpdateOperation();
            }
        }

        ArrayList<RecordUpdate> myArrayList = new ArrayList<>(deadKeys.size() + activeKeys.size() + insertOnlyUpdates.size());
        myArrayList.addAll(insertOnlyUpdates);
        myArrayList.addAll(deadKeys.values());
        myArrayList.addAll(activeKeys.values());
        return myArrayList;
    }
}
