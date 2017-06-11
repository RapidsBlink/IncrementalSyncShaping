package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.Server;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by yche on 6/8/17.
 */
public class ServerPipelinedComputation {
    // input parameters
    private static long pkLowerBound;
    private static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
    }

    public static void initSchemaTable(String schema, String table) {
        RecordLazyEval.schema = schema;
        RecordLazyEval.table = table;
    }

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

    // intermediate result
    final static Map<Long, RecordUpdate> inRangeActiveKeys = new HashMap<>();
    final static Set<Long> outOfRangeActiveKeys = new HashSet<>();
    final static Set<Long> deadKeys = new HashSet<>();

    // final result
    static ArrayList<String> filedList = new ArrayList<>();
    public final static Map<Long, String> inRangeRecord = new TreeMap<>();

    // computation
    private static SequentialRestore sequentialRestore = new SequentialRestore();

    // io and computation sync related
    private final static ExecutorService pageCachePool = Executors.newSingleThreadExecutor();
    private final static ExecutorService computationPool = Executors.newSingleThreadExecutor();
    private final static int TRANSFORM_WORKER_NUM = 8;
    private final static ExecutorService transformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);

    private static StringTaskBuffer strTaskBuffer = new StringTaskBuffer();

    public interface FindResultListener {
        void sendToClient(String result);
    }

    public static void readFilesIntoPageCache(final ArrayList<String> fileList) throws IOException {
        pageCachePool.execute(new Runnable() {
            @Override
            public void run() {
                long startTime = System.currentTimeMillis();

                for (String filePath : fileList) {
                    try {
                        FileUtil.readFileIntoPageCache(filePath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                long endTime = System.currentTimeMillis();
                System.out.println("read files into page cache, cost: " + (endTime - startTime) + "ms");
                if (Server.logger != null) {
                    Server.logger.info("read files into page cache, cost: " + (endTime - startTime) + "ms");
                }
            }
        });
    }

    private static class StringTaskBuffer {
        static int MAX_SIZE = 400000; // 0.4M
        private String[] stringArr = new String[MAX_SIZE];    // 100B*0.1M=40MB
        private int nextIndex = 0;

        private void addData(String line) {
            stringArr[nextIndex] = line;
            nextIndex++;
        }

        boolean isFull() {
            return nextIndex >= MAX_SIZE;
        }

        public int length() {
            return nextIndex;
        }

        public String get(int idx) {
            return stringArr[idx];
        }
    }

    private static class RecordLazyEvalTaskBuffer {
        final private RecordLazyEval[] recordLazyEvals;    // 100B*0.1M=10MB
        int nextIndex = 0;

        RecordLazyEvalTaskBuffer(int taskSize) {
            this.recordLazyEvals = new RecordLazyEval[taskSize];
        }

        void addData(RecordLazyEval recordLazyEval) {
            recordLazyEvals[nextIndex] = recordLazyEval;
            nextIndex++;
        }

        public int length() {
            return nextIndex;
        }

        public RecordLazyEval get(int idx) {
            return recordLazyEvals[idx];
        }
    }

    private static class TransformTask implements Callable<RecordLazyEvalTaskBuffer> {
        private StringTaskBuffer taskBuffer;
        private int startIdx; // inclusive
        private int endIdx;   // exclusive

        public TransformTask(StringTaskBuffer taskBuffer, int startIdx, int endIdx) {
            this.taskBuffer = taskBuffer;
            this.startIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        public RecordLazyEvalTaskBuffer call() throws Exception {
            StringBuilder stringBuilder = new StringBuilder();
            RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer = new RecordLazyEvalTaskBuffer(endIdx - startIdx);
            for (int i = startIdx; i < endIdx; i++) {
                recordLazyEvalTaskBuffer.addData(new RecordLazyEval(taskBuffer.get(i), stringBuilder));
            }
            return recordLazyEvalTaskBuffer;
        }
    }

    private static class ComputationTask implements Runnable {
        private StringTaskBuffer taskBuffer;
        private FindResultListener findResultListener;

        ComputationTask(StringTaskBuffer taskBuffer, FindResultListener findResultListener) {
            this.taskBuffer = taskBuffer;
            this.findResultListener = findResultListener;
        }

        @Override
        public void run() {
            Future<RecordLazyEvalTaskBuffer>[] futureArr = new Future[TRANSFORM_WORKER_NUM];
            int avgTaskNum = taskBuffer.length() / TRANSFORM_WORKER_NUM;
            int startIndex;
            int endIndex;
            for (int i = 0; i < TRANSFORM_WORKER_NUM; i++) {
                startIndex = i * avgTaskNum;
                endIndex = (i == TRANSFORM_WORKER_NUM - 1) ? taskBuffer.length() : (i + 1) * avgTaskNum;
                futureArr[i] = transformPool.submit(new TransformTask(taskBuffer, startIndex, endIndex));
            }
            for (int i = 0; i < TRANSFORM_WORKER_NUM; i++) {
                RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer = null;
                while (recordLazyEvalTaskBuffer == null) {
                    try {
                        recordLazyEvalTaskBuffer = futureArr[i].get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                for (int j = 0; j < recordLazyEvalTaskBuffer.length(); j++) {
                    String result = sequentialRestore.compute(recordLazyEvalTaskBuffer.get(j));
                    if (result != null) {
                        findResultListener.sendToClient(result);
                    }
                }
            }
        }
    }

    public static void OneRoundComputation(String fileName, FindResultListener findResultListener) throws IOException {
        long startTime = System.currentTimeMillis();

        ReversedLinesDirectReader reversedLinesFileReader = new ReversedLinesDirectReader(fileName);
        String line;
        long lineCount = 0;
        while ((line = reversedLinesFileReader.readLine()) != null) {
            if (strTaskBuffer.isFull()) {
                computationPool.execute(new ComputationTask(strTaskBuffer, findResultListener));
                strTaskBuffer = new StringTaskBuffer();
            }
            strTaskBuffer.addData(line);
            lineCount += line.length();
        }
        computationPool.execute(new ComputationTask(strTaskBuffer, findResultListener));

        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
        if (Server.logger != null) {
            Server.logger.info("computation time:" + (endTime - startTime));
            Server.logger.info("Byte count: " + lineCount);
        }
    }

    public static void JoinComputationThread() {
        // update computationPool states
        transformPool.shutdown();
        try {
            transformPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        computationPool.shutdown();
        pageCachePool.shutdown();
        // join threads
        try {
            computationPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            pageCachePool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

