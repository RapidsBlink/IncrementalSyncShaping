package com.alibaba.middleware.race.sync.server;


import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;

/**
 * Created by yche on 6/6/17.
 */
public class RecordUpdate {
    long firstKey;
    final long lastKey;
    boolean isFirstInsertInChunk;
    final boolean isLastDeleteInChunk;

    // lazy construction
    private Map<String, Object> filedUpdateMap = null;

    // used for update
    public RecordUpdate(RecordLazyEval recordLazyEval) {
        if (recordLazyEval.operationType == DELETE_OPERATION) {
            isLastDeleteInChunk = true;
            isFirstInsertInChunk = false;
            firstKey = recordLazyEval.prevPKVal;
            lastKey = recordLazyEval.prevPKVal;
        } else if (recordLazyEval.operationType == INSERT_OPERATION) {
            isLastDeleteInChunk = false;
            isFirstInsertInChunk = true;
            firstKey = recordLazyEval.curPKVal;
            lastKey = recordLazyEval.curPKVal;
        } else {
            isLastDeleteInChunk = false;
            isFirstInsertInChunk = false;
            firstKey = recordLazyEval.prevPKVal;
            lastKey = recordLazyEval.curPKVal;
        }
    }

    public void setFirstKey(long firstKey) {
        this.firstKey = firstKey;
    }

    public void addEntryIfNotThere(String key, Object value) {
        if (filedUpdateMap == null) {
            filedUpdateMap = new HashMap<>();
        }
        if (!filedUpdateMap.containsKey(key)) {
            filedUpdateMap.put(key, value);
        }
    }
}
