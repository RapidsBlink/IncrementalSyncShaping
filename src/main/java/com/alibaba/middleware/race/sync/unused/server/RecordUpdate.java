package com.alibaba.middleware.race.sync.unused.server;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;

/**
 * Created by yche on 6/6/17.
 */
public class RecordUpdate {
    public long firstKey;
    public final long lastKey;
    public boolean isFirstInsertInChunk;
    public final boolean isLastDeleteInChunk;

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

    void addEntriesIfNotThere(RecordLazyEval recordLazyEval) {
        if (filedUpdateMap == null) {
            filedUpdateMap = new HashMap<>();
        }
        while (recordLazyEval.hasNext()) {
            Map.Entry<String, Object> entry = recordLazyEval.next();
            if (!filedUpdateMap.containsKey(entry.getKey())) {
                filedUpdateMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public boolean isKeyChanged() {
        return firstKey != lastKey;
    }

    public void mergeAnotherIfPossible(RecordUpdate recordUpdate) {
        if (recordUpdate.filedUpdateMap != null) {
            for (Map.Entry<String, Object> entry : recordUpdate.filedUpdateMap.entrySet()) {
                addEntryIfNotThere(entry.getKey(), entry.getValue());
            }
        }
    }

    public String toOneLineString(ArrayList<String> fieldList) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(lastKey).append('\t');
        for (String field : fieldList) {
            stringBuilder.append(filedUpdateMap.get(field)).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
