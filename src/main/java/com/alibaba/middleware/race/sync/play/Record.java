package com.alibaba.middleware.race.sync.play;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yche on 6/6/17.
 */
public class Record {
    Long primaryKey;
    Map<String, Object> keyValueMap = new HashMap<>();

    // first `|` is optional
    // | binlog id | timestamp | schema | table | ...
    // column info | prev val | cur val
    Record(String recordStr) {
        int curIndex = 0;
        int fieldIndex = 0;
        while (recordStr.charAt(curIndex) == 0) {

        }
    }
}
