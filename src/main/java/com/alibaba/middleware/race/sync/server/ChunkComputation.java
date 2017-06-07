package com.alibaba.middleware.race.sync.server;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;

/**
 * Created by yche on 6/7/17.
 * used by a single thread
 */
public class ChunkComputation {
    private Map<Long, RecordUpdate> activeKeys = new HashMap<>();
    private Map<Long, RecordUpdate> deadKeys = new HashMap<>();
    private RecordLazyEval recordLazyEval;
    private StringBuilder stringBuilder = new StringBuilder();

    // used for each chunk computation
    public ArrayList<RecordUpdate> compute(ArrayList<String> fileChunk, int upper_idx, int lower_idx) {
        activeKeys.clear();
        deadKeys.clear();
        for (int i = upper_idx; i > lower_idx; i--) {
            recordLazyEval = new RecordLazyEval(fileChunk.get(i), stringBuilder);

            if (recordLazyEval.operationType == DELETE_OPERATION) {
                // because it is a delete-operation
                // => must-be the last operation for the corresponding record
                deadKeys.put(recordLazyEval.prevPKVal, new RecordUpdate(recordLazyEval.prevPKVal, recordLazyEval.prevPKVal));
            } else {

            }
        }
        return null;
    }
}
