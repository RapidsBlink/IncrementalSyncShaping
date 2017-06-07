package com.alibaba.middleware.race.sync.server;


import java.util.Map;

/**
 * Created by yche on 6/6/17.
 */
public class RecordUpdate {
    Long firstKey;
    Long lastKey;

    boolean isFirstOperationInsert = false;
    boolean isLastOperationDelete = false;

    Map<String, Object> filedUpdateMap = null;

    public RecordUpdate(Long firstKey, Long lastKey) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }
}
