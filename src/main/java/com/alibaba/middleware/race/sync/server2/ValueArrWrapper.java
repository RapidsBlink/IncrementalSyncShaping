package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;

/**
 * Created by yche on 6/18/17.
 */
public class ValueArrWrapper {
    public byte[][] valueIndexArr;

    ValueArrWrapper() {
        valueIndexArr = new byte[RecordField.FILED_NUM][];
    }

    public byte[] get(int index) {
        return valueIndexArr[index];
    }

    private void set(int index, byte[] indexPair) {
        valueIndexArr[index] = indexPair;
    }

    // used by transform worker
    void addIndex(ByteBuffer keyBytes, byte[] bytes) {
//        System.out.println("index:" + RecordField.fieldIndexMap.get(keyBytes) + "\t\tbuffer:" + new String(keyBytes.array(), 0, keyBytes.limit()));
        valueIndexArr[RecordField.fieldIndexMap.get(keyBytes)] = bytes;
    }

    void mergeLatterOperation(ValueArrWrapper valueIndexArr) {
        // null only when there is only pk update without other filed updates
        if (valueIndexArr != null) {
            for (int i = 0; i < RecordField.FILED_NUM; i++) {
                if (valueIndexArr.get(i) != null) {
                    this.set(i, valueIndexArr.get(i));
                }
            }
        }
    }
}
