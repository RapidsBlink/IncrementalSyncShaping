package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;

/**
 * Created by yche on 6/18/17.
 */
public class ValueIndexArrWrapper {
    private IndexPair[] valueIndexArr;

    public ValueIndexArrWrapper() {
        valueIndexArr = new IndexPair[RecordField.FILED_NUM];
        for (IndexPair indexPair : valueIndexArr) {
            System.out.println(indexPair);
        }
    }

    private IndexPair get(int index) {
        return valueIndexArr[index];
    }

    private void set(int index, IndexPair indexPair) {
        valueIndexArr[index] = indexPair;
    }

    public void addIndex(ByteBuffer keyBytes, long offset, short length) {
        valueIndexArr[RecordField.fieldIndexMap.get(keyBytes)] = new IndexPair(offset, length);
    }

    public void addGlobalOffset(long prevGlobalLen) {
        for (IndexPair indexPair : valueIndexArr) {
            if (indexPair != null) {
                indexPair.offset += prevGlobalLen;
            }
        }
    }

    public void mergeLatterOperation(ValueIndexArrWrapper valueIndexArr) {
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            if (valueIndexArr.get(i) != null) {
                this.set(i, valueIndexArr.get(i));
            }
        }
    }
}
