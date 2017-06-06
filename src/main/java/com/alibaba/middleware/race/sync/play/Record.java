package com.alibaba.middleware.race.sync.play;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.*;

/**
 * Created by yche on 6/6/17.
 */
public class Record {
    public String schemaStr;
    public String tableStr;
    public String operationType;

    public String primaryKeyStr;
    public Long primaryKeyCurrVal;
    public Long primaryKeyPrevVal;

    public final Map<String, Object> keyValueMap = new HashMap<>();

    public final ArrayList<String> colOrder = new ArrayList<>();

    private int curIndex;

    private String getNextString(String recordStr, StringBuilder stringBuilder) {
        if (recordStr.charAt(curIndex) == SPLIT_CHAR)
            curIndex++;

        char ch;
        stringBuilder.setLength(0);
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

    // attention: first `|` is optional
    // overall structure: | binlog id | timestamp | schema | table | ...
    // column structure: column info | prev val | cur val
    Record(String recordStr, boolean isKeepColOrder) {
        curIndex = 0;
        char ch;
        StringBuilder stringBuilder = new StringBuilder();

        // 1st: skip binlog id, and timestamp
        skipNextString(recordStr);
        skipNextString(recordStr);

        // 2nd: get schema and table
        schemaStr = getNextString(recordStr, stringBuilder);
        tableStr = getNextString(recordStr, stringBuilder);
        operationType = getNextString(recordStr, stringBuilder);

        // 3rd: get all field info and prev/cur values
        for (; curIndex < recordStr.length() - 1; ) {
            String key;
            String value;
            String valType;
            String keyType;

            if (recordStr.charAt(curIndex) == SPLIT_CHAR)
                curIndex++;

            // key_str
            stringBuilder.setLength(0);
            while ((ch = recordStr.charAt(curIndex)) != FIELD_SPLIT_CHAR) {
                curIndex++;
                stringBuilder.append(ch);
            }
            key = stringBuilder.toString();

            // record colOrder if it is required
            if (isKeepColOrder) {
                colOrder.add(key);
            }

            // val_type
            curIndex++;
            stringBuilder.setLength(0);
            stringBuilder.append(recordStr.charAt(curIndex));
            valType = stringBuilder.toString();

            // key_type
            curIndex += 2;
            stringBuilder.setLength(0);
            stringBuilder.append(recordStr.charAt(curIndex));
            keyType = stringBuilder.toString();

            if (keyType.equals(IS_PRIMARY_KEY)) {
                primaryKeyStr = key;
            }
            curIndex++;

            // prev val, only fetching for primary key
            if (keyType.equals(IS_PRIMARY_KEY)) {
                String prevVal = getNextString(recordStr, stringBuilder);
                primaryKeyPrevVal = prevVal.equals("NULL") ? -1 : Long.valueOf(prevVal);
            } else {
                skipNextString(recordStr);
            }

            // cur val
            value = getNextString(recordStr, stringBuilder);
            if (valType.equals(IS_NUMBER)) {
                keyValueMap.put(key, Long.valueOf(value));
            } else {
                keyValueMap.put(key, value);
            }

            // record cur val iff it is primary key
            if (keyType.equals(IS_PRIMARY_KEY)) {
                primaryKeyCurrVal = Long.valueOf(value);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String record = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        Record record1 = new Record(record, true);
        ObjectMapper mapper = new ObjectMapper();
        String jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record1);
        System.out.println(jsonInString);
    }
}
