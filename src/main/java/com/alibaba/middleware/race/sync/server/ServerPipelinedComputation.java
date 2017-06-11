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
    private final static ExecutorService mediatorPool = Executors.newSingleThreadExecutor();
    private final static ExecutorService computationPool = Executors.newSingleThreadExecutor();
    private final static int TRANSFORM_WORKER_NUM = 8;
    private final static ExecutorService transformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);

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
        static int MAX_SIZE = 100000; // 0.1M
        private String[] stringArr = new String[MAX_SIZE];    // 100B*0.1M=10MB
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

        TransformTask(StringTaskBuffer taskBuffer, int startIdx, int endIdx) {
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

    public interface FindResultListener {
        void sendToClient(String result);
    }

    private static class ComputationTask implements Runnable {
        private RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer;
        private FindResultListener findResultListener;

        ComputationTask(RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer, FindResultListener findResultListener) {
            this.recordLazyEvalTaskBuffer = recordLazyEvalTaskBuffer;
            this.findResultListener = findResultListener;
        }

        @Override
        public void run() {
            for (int j = 0; j < recordLazyEvalTaskBuffer.length(); j++) {
                String result = sequentialRestore.compute(recordLazyEvalTaskBuffer.get(j));
                if (result != null) {
                    findResultListener.sendToClient(result);
                }
            }
        }
    }

    private static class MediatorTask implements Runnable {
        private StringTaskBuffer taskBuffer;
        private FindResultListener findResultListener;
        private static Queue<Future<RecordLazyEvalTaskBuffer>> lazyEvalTaskQueue = new LinkedList<>();

        MediatorTask(StringTaskBuffer taskBuffer, FindResultListener findResultListener) {
            this.taskBuffer = taskBuffer;
            this.findResultListener = findResultListener;
        }

        static void syncConsumeReadyJobs(final FindResultListener findResultListener) {
            while (!lazyEvalTaskQueue.isEmpty()) {
                Future<RecordLazyEvalTaskBuffer> recordLazyEvalTaskBufferFuture = lazyEvalTaskQueue.poll();
                RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer = null;
                while (recordLazyEvalTaskBuffer == null) {
                    try {
                        recordLazyEvalTaskBuffer = recordLazyEvalTaskBufferFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                computationPool.execute(new ComputationTask(recordLazyEvalTaskBuffer, findResultListener));
            }
        }

        @Override
        public void run() {
            try {
                while (lazyEvalTaskQueue.size() > 0) {
                    Future<RecordLazyEvalTaskBuffer> futureWork = lazyEvalTaskQueue.peek();
                    if (futureWork.isDone()) {
                        computationPool.execute(new ComputationTask(futureWork.get(), findResultListener));
                        lazyEvalTaskQueue.poll();
                    } else {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Future<RecordLazyEvalTaskBuffer> recordLazyEvalTaskBufferFuture = transformPool.submit(new TransformTask(taskBuffer, 0, taskBuffer.length()));
            lazyEvalTaskQueue.add(recordLazyEvalTaskBufferFuture);
        }
    }

    public static void OneRoundComputation(String fileName, final FindResultListener findResultListener) throws IOException {
        long startTime = System.currentTimeMillis();

        ReversedLinesDirectReader reversedLinesFileReader = new ReversedLinesDirectReader(fileName);
        String line;
        long lineCount = 0;
        StringTaskBuffer strTaskBuffer = new StringTaskBuffer();
        while ((line = reversedLinesFileReader.readLine()) != null) {
            if (strTaskBuffer.isFull()) {
                mediatorPool.execute(new MediatorTask(strTaskBuffer, findResultListener));
                strTaskBuffer = new StringTaskBuffer();
            }
            strTaskBuffer.addData(line);
            lineCount += line.length();
        }
        mediatorPool.execute(new MediatorTask(strTaskBuffer, findResultListener));
        mediatorPool.execute(new Runnable() {
            @Override
            public void run() {
                MediatorTask.syncConsumeReadyJobs(findResultListener);
            }
        });

        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
        if (Server.logger != null) {
            Server.logger.info("computation time:" + (endTime - startTime));
            Server.logger.info("Byte count: " + lineCount);
        }
    }

    public static void JoinComputationThread() {
        // join page cache
        pageCachePool.shutdown();
        try {
            pageCachePool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // join transform states
        transformPool.shutdown();
        try {
            transformPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // join mediator
        mediatorPool.shutdown();
        try {
            mediatorPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // join computation
        computationPool.shutdown();
        try {
            computationPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

