package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation extends LogOperation {
    public byte[][] valueArr;

    public InsertOperation(long pk) {
        super(pk);
        valueArr = new byte[RecordField.FILED_NUM][];
    }

    public void addValue(ByteBuffer keyBytes, byte[] bytes) {
        valueArr[RecordField.fieldIndexMap.get(keyBytes)] = bytes;
    }

    public void addValue(int index, byte[] bytes) {
        valueArr[index] = bytes;
        if (index < 3)
            RecordScanner.max(bytes.length);
    }

    public void changePK(long newPk) {
        this.relevantKey = newPk;
    }

    public void mergeUpdate(UpdateOperation updateOperation) {
        if (updateOperation.valueArr != null) {
            for (int i = 0; i < RecordField.FILED_NUM; i++) {
                if (updateOperation.valueArr[i] != null) {
                    valueArr[i] = updateOperation.valueArr[i];
                }
            }
        }
    }

    String getOneLine() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(relevantKey).append('\t');
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            stringBuilder.append(new String(valueArr[i])).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
