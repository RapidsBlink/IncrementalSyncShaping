package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Created by yche on 6/16/17.
 * whole computation logic
 */
public class PipelinedComputation {
    private static long pkLowerBound;
    private static long pkUpperBound;

    private static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
    }

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

    // 1st phase
    static int CHUNK_SIZE = 16 * 1024 * 1024;
    private static int TRANSFORM_WORKER_NUM = 8;
    static ExecutorService fileTransformPool[] = new ExecutorService[TRANSFORM_WORKER_NUM];
    static ExecutorService computationPool = Executors.newFixedThreadPool(1);
    static ExecutorService dbUpdatePool[] = new ExecutorService[RestoreComputation.WORKER_NUM];

    static {
        for (int i = 0; i < TRANSFORM_WORKER_NUM; i++) {
            fileTransformPool[i] = Executors.newSingleThreadExecutor();
        }
        for (int i = 0; i < RestoreComputation.WORKER_NUM; i++) {
            dbUpdatePool[i] = Executors.newSingleThreadExecutor();
        }
    }

    static BlockingQueue<FileTransformMediatorTask> mediatorTasks = new ArrayBlockingQueue<>(1);

    // 2nd phase
    static int EVAL_WORKER_NUM = 16;
    private static ExecutorService evalSendPool;

    public static ConcurrentMap<Long, byte[]> finalResultMap;

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

    private static void firstPhaseComputation(ArrayList<String> srcFilePaths) throws IOException {
        // mediator
        ExecutorService mediatorPool = Executors.newFixedThreadPool(1);
        mediatorPool.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        FileTransformMediatorTask fileTransformMediatorTask = mediatorTasks.take();
                        if (fileTransformMediatorTask.isFinished)
                            break;
                        fileTransformMediatorTask.transform();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        // master thread: mmap reader
        for (String pathString : srcFilePaths) {
            MmapReader mmapReader = new MmapReader(pathString);
            mmapReader.fetchChunks();
            if (evalSendPool == null) {
                evalSendPool = Executors.newFixedThreadPool(EVAL_WORKER_NUM);
                finalResultMap = new ConcurrentSkipListMap<>();
            }
        }

        // join mediator
        try {
            mediatorTasks.put(new FileTransformMediatorTask());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        joinSinglePool(mediatorPool);

        // join tokenizer
        for (int i = 0; i < fileTransformPool.length; i++)
            joinSinglePool(fileTransformPool[i]);

        // join computation
        joinSinglePool(computationPool);
        for (int i = 0; i < dbUpdatePool.length; i++) {
            joinSinglePool(dbUpdatePool[i]);
        }
    }

    private static void secondPhaseComputation() {
        RestoreComputation.parallelEvalAndSend(evalSendPool);
        joinSinglePool(evalSendPool);
    }

    public static void globalComputation(ArrayList<String> srcFilePaths, long start, long end) throws IOException {
        if (Server.logger != null) {
            Server.logger.info("first phase start:" + String.valueOf(System.currentTimeMillis()));
        }
        initRange(start, end);
        firstPhaseComputation(srcFilePaths);
        if (Server.logger != null) {
            Server.logger.info("first phase end:" + String.valueOf(System.currentTimeMillis()));
        }
        secondPhaseComputation();
        if (Server.logger != null) {
            Server.logger.info("second phase end:" + String.valueOf(System.currentTimeMillis()));
        }
    }

    public static void putThingsIntoByteBuffer(ByteBuffer byteBuffer) {
        for (byte[] bytes : finalResultMap.values()) {
            byteBuffer.put(bytes);
        }
    }
}
