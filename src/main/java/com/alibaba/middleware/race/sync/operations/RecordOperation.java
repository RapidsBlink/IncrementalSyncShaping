package com.alibaba.middleware.race.sync.operations;

import com.alibaba.middleware.race.sync.server2.RecordField;

/**
 * Created by yche on 6/20/17.
 */
public class RecordOperation {
    public byte operationType;
    public FieldValueEval[] filedValuePointers;

    public RecordOperation(byte operationType) {
        this.operationType = operationType;
        this.filedValuePointers = null;
    }

    public String getOneLine(long key) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key).append('\t');
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            stringBuilder.append(filedValuePointers[i]).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
