package com.alibaba.middleware.race.sync.server;

import java.util.ArrayList;

import static com.alibaba.middleware.race.sync.Constants.FIELD_SPLIT_CHAR;
import static com.alibaba.middleware.race.sync.Constants.SPLIT_CHAR;

/**
 * Created by yche on 6/10/17.
 */
public class RecordFields {
    public final ArrayList<String> colOrder = new ArrayList<>();

    private int curIndex;

    private void skipNextString(String recordStr) {
        if (recordStr.charAt(curIndex) == SPLIT_CHAR)
            curIndex++;
        while (recordStr.charAt(curIndex) != SPLIT_CHAR) {
            curIndex++;
        }
    }

    private void initAllFieldInfo(String recordStr, StringBuilder stringBuilder) {
        int count = 0;
        for (; curIndex < recordStr.length() - 1; ) {
            if (recordStr.charAt(curIndex) == SPLIT_CHAR)
                curIndex++;

            // field info 1: key_str
            stringBuilder.setLength(0);
            char ch;
            while ((ch = recordStr.charAt(curIndex)) != FIELD_SPLIT_CHAR) {
                curIndex++;
                stringBuilder.append(ch);
            }

            // record colOrder if it is required
            if (count > 0) {
                colOrder.add(stringBuilder.toString());
            }
            count++;

            // field info 2: val_type, field info 3: key_type
            curIndex += 4;
            // prev val
            skipNextString(recordStr);
            // cur val
            skipNextString(recordStr);

        }
    }

    // overall structure: | binlog id | timestamp | schema | table | ...
    // column structure: column info | prev val | cur val
    public RecordFields(String recordStr) {
        curIndex = 0;
        StringBuilder stringBuilder = new StringBuilder();

        // 1st: skip binlog id, and timestamp, schema and table, operation type
        for (int i = 0; i < 5; i++)
            skipNextString(recordStr);

        initAllFieldInfo(recordStr, stringBuilder);
    }
}
