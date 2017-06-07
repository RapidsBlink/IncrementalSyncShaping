package com.alibaba.middleware.race.sync.server;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.*;

/**
 * Created by yche on 6/6/17.
 */
public class RecordLazyEval {
    private String recordStr;
    private StringBuilder stringBuilder;
    private int curIndex;

    // eager evaluation
    public char operationType;
    public long curPKVal;
    public long prevPKVal;

    // lazy evaluation
    public final Map<String, Object> keyValueMap = new HashMap<>();

    // end at '|'
    private String getNextString() {
        if (recordStr.charAt(curIndex) == SPLIT_CHAR)
            curIndex++;

        stringBuilder.setLength(0);
        char tmpChar;
        while ((tmpChar = recordStr.charAt(curIndex)) != SPLIT_CHAR) {
            curIndex++;
            stringBuilder.append(tmpChar);
        }
        return stringBuilder.toString();
    }

    // end at '|'
    private void skipNextString() {
        if (recordStr.charAt(curIndex) == SPLIT_CHAR)
            curIndex++;
        while (recordStr.charAt(curIndex) != SPLIT_CHAR) {
            curIndex++;
        }
    }

    // overall structure: | binlog id | timestamp | schema | table | column structure ...
    // column structure: column info | prev val | cur val
    RecordLazyEval(String recordStr, StringBuilder stringBuilder) {
        this.curIndex = 0;
        this.stringBuilder = stringBuilder;
        this.recordStr = recordStr;

        // 1st: skip binlog id, and timestamp, schema and table
        for (int i = 0; i < 4; i++) {
            skipNextString();
        }

        // 2nd: eager evaluate operation type, primary key
        curIndex++;
        this.operationType = this.recordStr.charAt(curIndex);
        curIndex++;

        // 3rd: eager evaluate primary key previous value and current value
        skipNextString();
        if (this.operationType == INSERT_OPERATION) {
            this.prevPKVal = -1;
            skipNextString();
        } else {
            this.prevPKVal = Long.valueOf(getNextString());
        }

        if (this.operationType == DELETE_OPERATION) {
            this.curPKVal = this.prevPKVal;
        } else {
            this.curPKVal = Long.valueOf(getNextString());
        }
    }

    private static void prettyPrint(RecordLazyEval recordLazyEval) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(recordLazyEval);
        System.out.println(jsonInString);
    }


    public static void main(String[] args) throws IOException {
        String record = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        String record2 = "|mysql-bin.00001717148759|1496736165000|middleware3|student|D|id:1:1|1|NULL|";
        prettyPrint(new RecordLazyEval(record, new StringBuilder()));
        prettyPrint(new RecordLazyEval(record2, new StringBuilder()));
    }
}
