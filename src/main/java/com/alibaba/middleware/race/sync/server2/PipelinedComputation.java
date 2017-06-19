package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

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
    public static RestoreComputation restoreComputation = new RestoreComputation();

    static ExecutorService evalSendPool = Executors.newFixedThreadPool(16);

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
        for (String pathString : srcFilePaths) {
            FileTransformWriteMediator fileTransformWriteMediator = new FileTransformWriteMediator(pathString);
            fileTransformWriteMediator.transformFile();
        }
        joinSinglePool(fileTransformPool);
        joinSinglePool(computationPool);
    }

    public static void secondPhaseComputation() {
        restoreComputation.parallelEvalAndSend(evalSendPool);
        joinSinglePool(evalSendPool);
    }

    public static void globalComputation(ArrayList<String> srcFilePaths,
                                         FindResultListener findResultListener) throws IOException {
        firstPhaseComputation(srcFilePaths);
        secondPhaseComputation();
        PipelinedComputation.findResultListener = findResultListener;
    }
}
