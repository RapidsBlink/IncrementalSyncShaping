package com.alibaba.middleware.race.sync.server2;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateOperation extends LogOperation {
    public static AtomicInteger count = new AtomicInteger(0);

    public byte[][] valueArr;

    public UpdateOperation(long prevKey) {
        super(prevKey);
        count.incrementAndGet();
    }

    public void addValue(ByteBuffer keyBytes, byte[] bytes) {
        int index = RecordField.fieldIndexMap.get(keyBytes);
        if (valueArr == null)
            valueArr = new byte[RecordField.FILED_NUM][];
        valueArr[index] = bytes;
        RecordScanner.max(bytes.length, index);
    }

    private void addLastName(byte[] bytes) {


    }

    private void addFirstName(byte[] bytes) {

    }

    private void addSex(byte[] bytes) {

    }

    private void addScore1(byte[] bytes) {

    }

    private void addScore2(byte[] bytes) {

    }


    public boolean isKeyChanged() {
        return this instanceof UpdateKeyOperation;
    }
}
