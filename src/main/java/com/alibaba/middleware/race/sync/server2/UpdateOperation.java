package com.alibaba.middleware.race.sync.server2;


import java.nio.ByteBuffer;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateOperation extends LogOperation {
    public byte[][] valueArr;

    public UpdateOperation(long prevKey) {
        super(prevKey);
    }

    public void addValue(ByteBuffer keyBytes, byte[] bytes) {
        if (valueArr == null)
            valueArr = new byte[RecordField.FILED_NUM][];
        valueArr[RecordField.fieldIndexMap.get(keyBytes)] = bytes;
    }

    public boolean isKeyChanged() {
        return this instanceof UpdateKeyOperation;
    }
}
