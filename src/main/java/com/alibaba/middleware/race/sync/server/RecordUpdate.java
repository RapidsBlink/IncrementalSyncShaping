package com.alibaba.middleware.race.sync.server;


import com.alibaba.middleware.race.sync.play.Record;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yche on 6/6/17.
 */
public class RecordUpdate {
    long firstKey;
    final long lastKey;

    // lazy construction
    private Map<String, Object> filedUpdateMap = null;

    // used for update
    public RecordUpdate(long firstKey, long lastKey) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }

    // used for insert and delete
    public RecordUpdate(long key) {
        this.firstKey = key;
        this.lastKey = key;
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
