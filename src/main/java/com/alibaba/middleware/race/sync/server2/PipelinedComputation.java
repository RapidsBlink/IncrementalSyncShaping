package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.*;

import static com.alibaba.middleware.race.sync.server2.FileTransformWriteMediator.bufferedOutputStream;

/**
 * Created by yche on 6/16/17.
 * whole computation logic
 */
public class PipelinedComputation {
    static int CHUNK_SIZE = 64 * 1024 * 1024;
    static int TRANSFORM_WORKER_NUM = 16;
    static ExecutorService fileTransformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);
    static ExecutorService computationPool = Executors.newSingleThreadExecutor();
    static RestoreComputation restoreComputation = new RestoreComputation();

    static ExecutorService evalSendPool = Executors.newFixedThreadPool(16);

    static BlockingQueue<Byte> writeQueue = new ArrayBlockingQueue<>(800);
    static ExecutorService writeFilePool = Executors.newSingleThreadExecutor();

    static PropertyValueFetcher propertyValueFetcher;
    static FindResultListener findResultListener;
    static final ConcurrentMap<Long, String> finalResultMap = new ConcurrentSkipListMap<>();

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

    // should be called after all files transformed
    public static void joinFirstPhasePool() {
        writeFilePool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    bufferedOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    if (Server.logger != null) {
                        Server.logger.info("assign task exception");
                        Server.logger.info(e.getMessage());
                    }
                }
            }
        });
        joinSinglePool(fileTransformPool);
        joinSinglePool(writeFilePool);
        joinSinglePool(computationPool);
    }

    public static void firstPhaseComputation(ArrayList<String> srcFilePaths, String dstFilePath) throws IOException {
        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(dstFilePath));
        for (String pathString : srcFilePaths) {
            FileTransformWriteMediator fileTransformWriteMediator = new FileTransformWriteMediator(pathString);
            fileTransformWriteMediator.transformFile();
        }
        joinFirstPhasePool();
    }

    private static void secondPhaseComputation(String dstFilePath) {
        propertyValueFetcher = new PropertyValueFetcher(dstFilePath);
        restoreComputation.parallelEvalAndSend(evalSendPool);
        joinSinglePool(evalSendPool);
    }

    public static void globalComputation(ArrayList<String> srcFilePaths, String dstFilePath,
                                         FindResultListener findResultListener) throws IOException {
        firstPhaseComputation(srcFilePaths, dstFilePath);
        secondPhaseComputation(dstFilePath);
        PipelinedComputation.findResultListener = findResultListener;
    }
}
