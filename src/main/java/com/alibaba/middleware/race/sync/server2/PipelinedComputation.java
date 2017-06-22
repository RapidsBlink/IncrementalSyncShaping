package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Created by yche on 6/16/17.
 * whole computation logic
 */
public class PipelinedComputation {
    static int CHUNK_SIZE = 32 * 1024 * 1024;
    private static int TRANSFORM_WORKER_NUM = 16;
    static int WORK_NUM = TRANSFORM_WORKER_NUM * 4;
    static ExecutorService fileTransformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);
    public static RestoreComputation restoreComputation = new RestoreComputation();

    public static BlockingQueue<LogOperation[]> blockingQueue = new ArrayBlockingQueue<>(48);
    public static BlockingQueue<FileTransformMediatorTask> mediatorTasks = new ArrayBlockingQueue<>(4);

    static ExecutorService computationPool = Executors.newFixedThreadPool(1);
    static ExecutorService mediatorPool = Executors.newFixedThreadPool(1);

    private static ExecutorService evalSendPool = Executors.newFixedThreadPool(16);

    public static FindResultListener findResultListener;
    public static final ConcurrentMap<Long, String> finalResultMap = new ConcurrentSkipListMap<>();

    public interface FindResultListener {
        void sendToClient(String result);
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

    public static void firstPhaseComputation(ArrayList<String> srcFilePaths) throws IOException {
        computationPool.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        LogOperation[] logOperations = blockingQueue.take();
                        if (logOperations.length == 0)
                            break;
                        restoreComputation.compute(logOperations);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
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

        for (String pathString : srcFilePaths) {
            MmapReader mmapReader = new MmapReader(pathString);
            mmapReader.fetchChunks();
        }

        try {
            mediatorTasks.put(new FileTransformMediatorTask());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        joinSinglePool(mediatorPool);
        joinSinglePool(fileTransformPool);
        try {
            blockingQueue.put(new LogOperation[0]);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        joinSinglePool(computationPool);

    }

    public static void secondPhaseComputation() {
        restoreComputation.parallelEvalAndSend(evalSendPool);
        joinSinglePool(evalSendPool);
    }

    public static void globalComputation(ArrayList<String> srcFilePaths,
                                         FindResultListener findResultListener, long start, long end) throws IOException {
        if (Server.logger != null) {
            Server.logger.info("first phase start:" + String.valueOf(System.currentTimeMillis()));
        }
        initRange(start, end);
        PipelinedComputation.findResultListener = findResultListener;
        firstPhaseComputation(srcFilePaths);
        if (Server.logger != null) {
            Server.logger.info("first phase end:" + String.valueOf(System.currentTimeMillis()));
        }
        secondPhaseComputation();
        if (Server.logger != null) {
            Server.logger.info("second phase end:" + String.valueOf(System.currentTimeMillis()));
        }
    }

    private static long pkLowerBound;
    private static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
    }

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

}
