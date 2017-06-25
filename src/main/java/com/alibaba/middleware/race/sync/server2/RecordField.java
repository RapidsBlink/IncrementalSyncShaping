package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.Constants.FILED_SPLITTER;
import static com.alibaba.middleware.race.sync.Constants.LINE_SPLITTER;


/**
 * Created by yche on 6/17/17.
 * use once
 */
public class RecordField {
    public static Map<ByteBuffer, Integer> fieldIndexMap = new HashMap<>();
    static int[] fieldSkipLen;
    public static int FILED_NUM;
    static int KEY_LEN;

    public static boolean isInit() {
        return fieldIndexMap.size() > 0;
    }

    private int nextIndex = 0;
    private int nextFieldIndex = 0;
    private ByteBuffer mappedByteBuffer;
    private ByteBuffer myBuffer = ByteBuffer.allocate(1024);

    public RecordField(ByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }

    // stop at `|`
    private void skipField() {
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER) {
            nextIndex++;
        }
        while (mappedByteBuffer.get(nextIndex) != FILED_SPLITTER) {
            nextIndex++;
        }
    }

    // stop at `|`
    private ByteBuffer getNextField() {
        myBuffer.clear();
        if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER) {
            nextIndex++;
        }
        byte myByte;
        while ((myByte = mappedByteBuffer.get(nextIndex)) != FILED_SPLITTER) {
            myBuffer.put(myByte);
            nextIndex++;
        }
        myBuffer.flip();
        ByteBuffer retByteBuffer = ByteBuffer.allocate(myBuffer.limit());
        retByteBuffer.put(myBuffer);
        retByteBuffer.flip();
        return retByteBuffer;
    }

    public void initFieldIndexMap() {
        // mysql, ts, schema, table, op,
        for (int i = 0; i < 5; i++) {
            skipField();
        }
        // pk name
        ByteBuffer keyBuffer = getNextField();
        KEY_LEN = keyBuffer.limit();

        // prev val, cur val
        for (int i = 0; i < 2; i++) {
            skipField();
        }

        // peek next char after `|`
        while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
            ByteBuffer nextField = getNextField();
            fieldIndexMap.put(nextField, nextFieldIndex);
            nextFieldIndex++;
            skipField();
            skipField();
        }

        FILED_NUM = fieldIndexMap.size();
        fieldSkipLen = new int[FILED_NUM];
        for (Map.Entry<ByteBuffer, Integer> entry : fieldIndexMap.entrySet()) {
            fieldSkipLen[entry.getValue()] = entry.getKey().limit() + 1;
        }
    }
}
