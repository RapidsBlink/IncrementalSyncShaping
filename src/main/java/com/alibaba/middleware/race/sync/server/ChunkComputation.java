package com.alibaba.middleware.race.sync.server;

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

    // used for each chunk computation
    public void compute(ArrayList<String> fileChunk, int upper_idx, int lower_idx) {
        activeKeys.clear();
        deadKeys.clear();

        for (int i = upper_idx; i > lower_idx; i--) {
            recordLazyEval = new RecordLazyEval(fileChunk.get(i), stringBuilder);

            // 1st: delete-operation, 2nd: insert-operation, 3rd: update-operation
            if (recordLazyEval.operationType == DELETE_OPERATION) {
                // ?-delete mode, it is last operation for this record
                deadKeys.put(recordLazyEval.prevPKVal, new RecordUpdate(recordLazyEval.prevPKVal));
            } else if (recordLazyEval.operationType == INSERT_OPERATION) {
                if (deadKeys.containsKey(recordLazyEval.curPKVal)) {
                    // insert-delete mode, prune it
                    deadKeys.remove(recordLazyEval.curPKVal);
                } else {
                    RecordUpdate recordUpdate = activeKeys.get(recordLazyEval.curPKVal);
                    if (recordUpdate != null) {
                        // insert-update mode
                        recordUpdate.setFirstKey(recordLazyEval.curPKVal);
                    } else {
                        // insert-insert mode
                        recordUpdate = new RecordUpdate(recordLazyEval.curPKVal);
                        insertOnlyUpdates.add(recordUpdate);
                    }
                    // update contents if possible
                    while (recordLazyEval.hasNext()) {
                        AbstractMap.SimpleEntry<String, Object> entry = recordLazyEval.next();
                        recordUpdate.addEntryIfNotThere(entry.getKey(), entry.getValue());
                    }
                }
            } else if (recordLazyEval.operationType == UPDATE_OPERATION) {
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
                        recordUpdate = new RecordUpdate(recordLazyEval.prevPKVal, recordLazyEval.curPKVal);
                        activeKeys.put(recordLazyEval.prevPKVal, recordUpdate);
                    }

                    // update contents if possible
                    while (recordLazyEval.hasNext()) {
                        AbstractMap.SimpleEntry<String, Object> entry = recordLazyEval.next();
                        recordUpdate.addEntryIfNotThere(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }
}
