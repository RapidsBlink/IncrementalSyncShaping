package com.alibaba.middleware.race.sync.server;

import java.util.AbstractMap;
import java.util.Iterator;

import static com.alibaba.middleware.race.sync.Constants.*;

/**
 * Created by yche on 6/6/17.
 * provides iterator for lazy evaluation of updated entries except primary key
 */
public class RecordLazyEval implements Iterator<AbstractMap.SimpleEntry<String, Object>> {
    public static String schema;
    public static String table;

    String recordStr;
    private StringBuilder stringBuilder;
    private int curIndex;

    private boolean isSchemaTableValid = true;

    // eager evaluation
    public char operationType;
    public long curPKVal;
    public long prevPKVal;


    boolean isSchemaTableValid() {
        return isSchemaTableValid;
    }

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

    public boolean isPKUpdate() {
        return prevPKVal != curPKVal;
    }

    // overall structure: | binlog id | timestamp | schema | table | column structure ...
    // column structure: column info | prev val | cur val
    public RecordLazyEval(String recordStr) {
        this.curIndex = 0;
        this.recordStr = recordStr;
        this.stringBuilder=new StringBuilder();

        // 1st: skip: binlog id, and timestamp
        for (int i = 0; i < 2; i++) {
            skipNextString();
        }

        // check schema and table
        String schema = getNextString();

        if (!schema.equals(RecordLazyEval.schema)) {
            this.isSchemaTableValid = false;
            return;
        }

        String table = getNextString();
        if (!table.equals(RecordLazyEval.table)) {
            this.isSchemaTableValid = false;
            return;
        }

        // 2nd: eager evaluate: operation type, primary key
        curIndex++;
        this.operationType = this.recordStr.charAt(curIndex);
        curIndex++;

        // 3rd: eager evaluate: primary key previous value and current value
        skipNextString();
        if (this.operationType == INSERT_OPERATION) {
            this.prevPKVal = -1;
            skipNextString();
        } else {
            this.prevPKVal = Long.valueOf(getNextString());
        }

        if (this.operationType == DELETE_OPERATION) {
            this.curPKVal = this.prevPKVal;
            this.recordStr = null;
            this.stringBuilder = null;
        } else {
            this.curPKVal = Long.valueOf(getNextString());
        }

    }

    @Override
    public boolean hasNext() {
        return this.operationType != DELETE_OPERATION && curIndex + 1 <= recordStr.length() - 1;
    }

    @Override
    public AbstractMap.SimpleEntry<String, Object> next() {
        // skip '|'
        curIndex++;

        // field info 1: key_str
        char ch;
        stringBuilder.setLength(0);
        while ((ch = recordStr.charAt(curIndex)) != FIELD_SPLIT_CHAR) {
            curIndex++;
            stringBuilder.append(ch);
        }
        String key = stringBuilder.toString();

        // field info 2: val_type
        curIndex++;
        char valType = recordStr.charAt(curIndex);

        // skip: field info 3, key_type
        curIndex += 3;

        // skip: field prev val
        skipNextString();

        // field cur val
        if (valType == IS_NUMBER) {
            return new AbstractMap.SimpleEntry<String, Object>(key, Long.valueOf(getNextString()));
        } else {
            return new AbstractMap.SimpleEntry<String, Object>(key, getNextString());
        }
    }

    @Override
    public void remove() {
    }
}
