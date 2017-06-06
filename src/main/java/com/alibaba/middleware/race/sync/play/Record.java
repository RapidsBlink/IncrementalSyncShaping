package com.alibaba.middleware.race.sync.play;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.*;

/**
 * Created by yche on 6/6/17.
 */
public class Record {
    String primaryKeyStr;
    Long primaryKey;

    final Map<String, Object> keyValueMap = new HashMap<>();
    String schemaStr;
    String tableStr;
    String operationType;

    private int curIndex;

    private String getNextString(String recordStr) {
        StringBuilder stringBuilder = new StringBuilder();
        if (recordStr.charAt(curIndex) == SPLIT_CHAR)
            curIndex++;

        char ch;
        while ((ch = recordStr.charAt(curIndex)) != SPLIT_CHAR) {
            curIndex++;
            stringBuilder.append(ch);
        }
        return stringBuilder.toString();
    }

    private void skipNextString(String recordStr) {
        if (recordStr.charAt(curIndex) == SPLIT_CHAR)
            curIndex++;
        while (recordStr.charAt(curIndex) != SPLIT_CHAR) {
            curIndex++;
        }
    }

    // first `|` is optional
    // | binlog id | timestamp | schema | table | ...
    // column info | prev val | cur val
    Record(String recordStr) {
        curIndex = 0;

        // skip binlog id, and timestamp
        skipNextString(recordStr);
        skipNextString(recordStr);

        char ch;
        // get schema and table
        schemaStr = getNextString(recordStr);
        tableStr = getNextString(recordStr);
        operationType=getNextString(recordStr);

        StringBuilder stringBuilder = new StringBuilder();
        for (; curIndex < recordStr.length(); ) {
            String key;
            String value;
            String valType;
            String keyType;
            // field info
            if (recordStr.charAt(curIndex) == SPLIT_CHAR)
                curIndex++;

            // key_str, val_type, key_type
            stringBuilder.setLength(0);
            while ((ch = recordStr.charAt(curIndex)) != FIELD_SPLIT_CHAR) {
                curIndex++;
                stringBuilder.append(ch);
            }
            key = stringBuilder.toString();

            curIndex++;
            stringBuilder.setLength(0);
            stringBuilder.append(recordStr.charAt(curIndex));
            valType = stringBuilder.toString();

            curIndex += 2;
            stringBuilder.setLength(0);
            stringBuilder.append(recordStr.charAt(curIndex));
            keyType = stringBuilder.toString();

            if (keyType.equals(IS_PRIMARY_KEY)) {
                primaryKeyStr = key;
            }

            // prev val
            skipNextString(recordStr);

            // cur val
            value = getNextString(recordStr);
            if (valType.equals(IS_INTEGER)) {
                keyValueMap.put(key, Integer.valueOf(value));
            } else {
                keyValueMap.put(key, value);
            }
        }
    }

    public static void main(String[] args) {
        String record = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        Record record1 = new Record(record);
        for (Map.Entry<String, Object> entry : record1.keyValueMap.entrySet()) {
            System.out.println(entry.getKey() + "," + entry.getValue());
        }
    }
}
