package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/20/17.
 */
public class RecordOperation {
    byte operationType;
    FieldValueEval[] filedValuePointers;

    public RecordOperation(byte operationType) {
        this.operationType = operationType;
        this.filedValuePointers = null;
    }

    String getOneLine(long key) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key).append('\t');
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            stringBuilder.append(filedValuePointers[i]).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
