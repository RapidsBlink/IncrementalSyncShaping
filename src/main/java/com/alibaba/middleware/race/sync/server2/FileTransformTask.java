package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.Callable;

import static com.alibaba.middleware.race.sync.server2.FileTransform.FILED_SPLITTER;
import static com.alibaba.middleware.race.sync.server2.FileTransform.LINE_SPLITTER;

/**
 * Created by yche on 6/18/17.
 */
public class FileTransformTask implements Callable<ByteBuffer> {
    private final MappedByteBuffer mappedByteBuffer;
    private final int startIndex; // inclusive, not `\n` at first
    private final int endIndex;   // exclusive
    private final ByteBuffer byteBuffer; // fast-consumption object

    private int nextIndex;

    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.nextIndex = this.startIndex;
        this.byteBuffer = ByteBuffer.allocate(endIndex - startIndex);
    }

    // for the first small chunk
    FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.nextIndex = this.startIndex;
        this.byteBuffer = ByteBuffer.allocate(endIndex - startIndex);

        // computation
        int localIndex = 0;
        for (int i = 0; i < 4; i++) {
            if (remainingByteBuffer.get(localIndex) == FILED_SPLITTER)
                localIndex++;
            while (remainingByteBuffer.get(localIndex) != FILED_SPLITTER)
                localIndex++;
        }
        this.byteBuffer.put(remainingByteBuffer.array(), localIndex, remainingByteBuffer.limit() - localIndex);
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

    private void putIntoByteBufferUntilNewLine() {
        byte myByte;
        while ((myByte = mappedByteBuffer.get(nextIndex)) != LINE_SPLITTER) {
            byteBuffer.put(myByte);
            nextIndex++;
        }
        // add `\n`
        byteBuffer.put(mappedByteBuffer.get(nextIndex));
        // stop at new start `|`, finally stop at unreachable endIndex
        nextIndex++;
    }

    @Override
    public ByteBuffer call() throws Exception {
        while (nextIndex < endIndex) {
            for (int i = 0; i < 4; i++) {
                skipField();
            }
            putIntoByteBufferUntilNewLine();
        }
        byteBuffer.flip();
        return byteBuffer;
    }
}