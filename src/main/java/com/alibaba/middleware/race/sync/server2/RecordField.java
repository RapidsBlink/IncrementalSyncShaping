package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.middleware.race.sync.server2.FileTransform.FILED_SPLITTER;
import static com.alibaba.middleware.race.sync.server2.FileTransform.LINE_SPLITTER;

/**
 * Created by yche on 6/17/17.
 * use once
 */
public class RecordField {
    public static Map<ByteBuffer, Integer> fieldIndexMap = new HashMap<>();

    public static boolean isInit() {
        return fieldIndexMap.size() > 0;
    }

    private int nextIndex = 0;
    private int nextFieldIndex = 0;
    private ByteBuffer mappedByteBuffer;
    private ByteBuffer myBuffer = ByteBuffer.allocate(16 * 1024);

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
        // mysql, ts, schema, table, op, pk name, prev val, cur val
        for (int i = 0; i < 8; i++) {
            skipField();
        }

        // peek next char after `|`
        while (mappedByteBuffer.get(nextIndex + 1) != LINE_SPLITTER) {
            ByteBuffer nextField=getNextField();
            fieldIndexMap.put(nextField, nextFieldIndex);
            nextFieldIndex++;
            skipField();
            skipField();
        }
    }
}
