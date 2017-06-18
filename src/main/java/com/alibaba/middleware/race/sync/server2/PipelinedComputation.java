package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.util.concurrent.*;

/**
 * Created by yche on 6/16/17.
 */
public class PipelinedComputation {
    static byte FILED_SPLITTER = '|';
    static byte LINE_SPLITTER = '\n';

    static int CHUNK_SIZE = 64 * 1024 * 1024;
    static int TRANSFORM_WORKER_NUM = 16;
    static ExecutorService fileTransformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);

    static BlockingQueue<Byte> writeQueue = new ArrayBlockingQueue<>(500);
    static ExecutorService writeFilePool = Executors.newSingleThreadExecutor();

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
