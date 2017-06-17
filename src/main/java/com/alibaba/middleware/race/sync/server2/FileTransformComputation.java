package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

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
    static byte LINE_SPLITTER = '\n';

    static int CHUNK_SIZE = 64 * 1024 * 1024;
    static int TRANSFORM_WORKER_NUM = 16;
    static ExecutorService fileTransformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);
    static ExecutorService writeFilePool = Executors.newSingleThreadExecutor();

    public static class FileTransformTask implements Callable<ByteBuffer> {
        final MappedByteBuffer mappedByteBuffer;
        final int startIndex; // inclusive, not `\n` at first
        final int endIndex;   // exclusive
        final ByteBuffer byteBuffer; // fast-consumption object

        private int nextIndex;

        public FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.nextIndex = this.startIndex;
            this.byteBuffer = ByteBuffer.allocate(endIndex - startIndex);
        }

        // for the first small chunk
        public FileTransformTask(MappedByteBuffer mappedByteBuffer, int startIndex, int endIndex, ByteBuffer remainingByteBuffer) {
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
            this.byteBuffer.put(remainingByteBuffer.array(), localIndex, remainingByteBuffer.limit()-localIndex);
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

    private static void joinSinglePool(ExecutorService executorService) {
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            if (Server.logger != null) {
                Server.logger.info(e.getMessage());
            }
        }
    }

    // should be called after all files transformed
    public static void joinPool() {
        joinSinglePool(fileTransformPool);
        joinSinglePool(writeFilePool);
    }
}
