package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;

/**
 * Created by yche on 6/18/17.
 */
public class ValueIndexArrWrapper {
    private IndexPair[] valueIndexArr;

    ValueIndexArrWrapper() {
        valueIndexArr = new IndexPair[RecordField.FILED_NUM];
    }

    public IndexPair get(int index) {
        return valueIndexArr[index];
    }

    private void set(int index, IndexPair indexPair) {
        valueIndexArr[index] = indexPair;
    }

    // used by transform worker
    void addIndex(ByteBuffer keyBytes, long offset, short length) {
        System.out.println("index:" + RecordField.fieldIndexMap.get(keyBytes) + "\t\tbuffer:" + new String(keyBytes.array(), 0, keyBytes.limit()));
        valueIndexArr[RecordField.fieldIndexMap.get(keyBytes)] = new IndexPair(offset, length);
    }

    // used by master thread
    public void addGlobalOffset(long prevGlobalLen) {
        for (IndexPair indexPair : valueIndexArr) {
            if (indexPair != null) {
                indexPair.offset += prevGlobalLen;
            }
        }
    }

    void mergeLatterOperation(ValueIndexArrWrapper valueIndexArr) {
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            if (valueIndexArr.get(i) != null) {
                this.set(i, valueIndexArr.get(i));
            }
        }
    }
}
