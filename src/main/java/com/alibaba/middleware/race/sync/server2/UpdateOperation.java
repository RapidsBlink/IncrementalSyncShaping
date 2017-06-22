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
        int index = RecordField.fieldIndexMap.get(keyBytes);
        if (valueArr == null)
            valueArr = new byte[RecordField.FILED_NUM][];
        valueArr[index] = bytes;
    }
}
