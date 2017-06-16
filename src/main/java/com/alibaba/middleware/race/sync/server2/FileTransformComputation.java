package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by yche on 6/16/17.
 */
public class FileTransformComputation {
    private static byte FILED_SPLITTER = '|';
    private static byte LINE_SPLITTER = '\n';

    static int CHUNK_SIZE = 64 * 1024 * 1024;
    private static int TRANSFORM_WORKER_NUM = 16;
    private static int SMALL_CHUNK_SIZE = CHUNK_SIZE / TRANSFORM_WORKER_NUM;
    static ExecutorService transformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);
    static ExecutorService writeFilePool = Executors.newSingleThreadExecutor();

    public static class TransformTask implements Callable<ByteBuffer> {
        final MappedByteBuffer mappedByteBuffer;
        final int startIndex; // inclusive, not `\n` at first
        final int endIndex;   // exclusive
        final ByteBuffer byteBuffer; // fast-consumption object

        private int nextIndex;

        public TransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.nextIndex = this.startIndex;
            this.byteBuffer = ByteBuffer.allocate(endIndex - startIndex);
        }

        // stop at `|`
        private void skipField() {
            if (mappedByteBuffer.get(nextIndex) == FILED_SPLITTER) {
                nextIndex++;
            }
            while (mappedByteBuffer.getChar(nextIndex) != FILED_SPLITTER) {
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

    // should be called after all files transformed
    public static void joinPool() {
        transformPool.shutdown();
        try {
            transformPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        writeFilePool.shutdown();
        try {
            writeFilePool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
