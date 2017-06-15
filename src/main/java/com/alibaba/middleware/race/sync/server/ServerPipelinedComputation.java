package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.Server;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.middleware.race.sync.Constants.INSERT_OPERATION;

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

//    public static void initSchemaTable(String schema, String table) {
//        RecordLazyEval.schema = schema;
//        RecordLazyEval.table = table;
//    }

    private static FindResultListener findResultListener;

    public static void initFindResultListener(FindResultListener findResultListener) {
        ServerPipelinedComputation.findResultListener = findResultListener;
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
    public final static ConcurrentMap<Long, String> inRangeRecord = new ConcurrentSkipListMap<>();

    // sequential computation model
    private static SequentialRestore sequentialRestore = new SequentialRestore();

    // type2 pool: transform and computation
    private final static ExecutorService transCompMediatorPool = Executors.newSingleThreadExecutor();
    private final static int TRANSFORM_WORKER_NUM = 8;
    private final static ExecutorService transformPool = Executors.newFixedThreadPool(TRANSFORM_WORKER_NUM);
    private final static ExecutorService computationPool = Executors.newSingleThreadExecutor();

    // type3 pool: eval update application computation
    final static int EVAL_UPDATE_WORKER_NUM = 8;
    final static ExecutorService[] evalUpdateApplyPools = new ExecutorService[EVAL_UPDATE_WORKER_NUM];
    final static EvalUpdateTaskBuffer[] evalUpdateApplyTasks = new EvalUpdateTaskBuffer[EVAL_UPDATE_WORKER_NUM];

    static {
        for (int i = 0; i < EVAL_UPDATE_WORKER_NUM; i++) {
            evalUpdateApplyPools[i] = Executors.newSingleThreadExecutor();
            evalUpdateApplyTasks[i] = new EvalUpdateTaskBuffer();
        }
    }

    // task buffer: ByteArrTaskBuffer, StringTaskBuffer, RecordLazyEvalTaskBuffer
    // EvalUpdateTaskBuffer
    public static class ByteArrTaskBuffer {
        static int MAX_SIZE = 40000; // tuning it.................
        private byte[][] byteArrArr = new byte[MAX_SIZE][];
        private int nextIndex = 0;

        private void addData(byte[] line) {
            byteArrArr[nextIndex] = line;
            nextIndex++;
        }

        boolean isFull() {
            return nextIndex >= MAX_SIZE;
        }

        public int length() {
            return nextIndex;
        }

        public byte[] get(int idx) {
            return byteArrArr[idx];
        }
    }

    public static class RecordLazyEvalTaskBuffer {
        final private RecordLazyEval[] recordLazyEvals;
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

    static class EvalUpdate {
        final RecordUpdate recordUpdate;
        final RecordLazyEval recordLazyEval;

        EvalUpdate(RecordUpdate recordUpdate, RecordLazyEval recordLazyEval) {
            this.recordUpdate = recordUpdate;
            this.recordLazyEval = recordLazyEval;
        }
    }

    public static class EvalUpdateTaskBuffer {
        private static int MAX_SIZE = 4000;
        final private EvalUpdate[] evalUpdates;
        int nextIndex = 0;

        EvalUpdateTaskBuffer() {
            this.evalUpdates = new EvalUpdate[MAX_SIZE];
        }

        void addData(RecordUpdate recordUpdate, RecordLazyEval recordLazyEval) {
            evalUpdates[nextIndex] = new EvalUpdate(recordUpdate, recordLazyEval);
            nextIndex++;
        }

        public int length() {
            return nextIndex;
        }

        boolean isFull() {
            return nextIndex >= MAX_SIZE;
        }

        public EvalUpdate get(int idx) {
            return evalUpdates[idx];
        }
    }

    // tasks type1: DecodeDispatchTask, DecodeTask,
    // tasks type2: TransCompMediatorTask, TransformTask, ComputationTask
    private static class TransCompMediatorTask implements Runnable {
        private ByteArrTaskBuffer taskBuffer;
        private static Queue<Future<RecordLazyEvalTaskBuffer>> lazyEvalTaskQueue = new LinkedList<>();

        TransCompMediatorTask(ByteArrTaskBuffer taskBuffer) {
            this.taskBuffer = taskBuffer;
        }

        static void syncConsumeReadyJobs() {
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
                computationPool.execute(new ComputationTask(recordLazyEvalTaskBuffer));
            }
            computationPool.execute(new Runnable() {
                @Override
                public void run() {
                    sequentialRestore.flushTasksToPool();
                }
            });
        }

        @Override
        public void run() {
            try {
                while (lazyEvalTaskQueue.size() > 0) {
                    Future<RecordLazyEvalTaskBuffer> futureWork = lazyEvalTaskQueue.peek();
                    if (futureWork.isDone()) {
                        computationPool.execute(new ComputationTask(futureWork.get()));
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

    private static class TransformTask implements Callable<RecordLazyEvalTaskBuffer> {
        private ByteArrTaskBuffer taskBuffer;
        private int startIdx; // inclusive
        private int endIdx;   // exclusive

        TransformTask(ByteArrTaskBuffer taskBuffer, int startIdx, int endIdx) {
            this.taskBuffer = taskBuffer;
            this.startIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        public RecordLazyEvalTaskBuffer call() throws Exception {
            RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer = new RecordLazyEvalTaskBuffer(endIdx - startIdx);
            for (int i = startIdx; i < endIdx; i++) {
                recordLazyEvalTaskBuffer.addData(new RecordLazyEval(new String(taskBuffer.get(i))));
            }
            return recordLazyEvalTaskBuffer;
        }
    }

    public interface FindResultListener {
        void sendToClient(String result);
    }

    private static class ComputationTask implements Runnable {
        private RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer;

        ComputationTask(RecordLazyEvalTaskBuffer recordLazyEvalTaskBuffer) {
            this.recordLazyEvalTaskBuffer = recordLazyEvalTaskBuffer;
        }

        @Override
        public void run() {
            for (int j = 0; j < recordLazyEvalTaskBuffer.length(); j++) {
                sequentialRestore.compute(recordLazyEvalTaskBuffer.get(j));
            }
        }
    }

    public static class EvalUpdateApplyTask implements Runnable {
        private EvalUpdateTaskBuffer evalUpdateTaskBuffer;

        EvalUpdateApplyTask(EvalUpdateTaskBuffer evalUpdateTaskBuffer) {
            this.evalUpdateTaskBuffer = evalUpdateTaskBuffer;
        }

        @Override
        public void run() {
            EvalUpdate evalUpdate;
            RecordUpdate recordUpdate;
            RecordLazyEval recordLazyEval;

            for (int i = 0; i < evalUpdateTaskBuffer.length(); i++) {
                evalUpdate = evalUpdateTaskBuffer.get(i);
                recordUpdate = evalUpdate.recordUpdate;
                recordLazyEval = evalUpdate.recordLazyEval;
                recordUpdate.addEntriesIfNotThere(recordLazyEval);
                if (recordLazyEval.operationType == INSERT_OPERATION) {
                    String result = recordUpdate.toOneLineString(filedList);
                    findResultListener.sendToClient(result);
                    inRangeRecord.put(recordUpdate.lastKey, result);
                }
            }
        }
    }


    public static void OneRoundComputation(String fileName) throws IOException {
        long startTime = System.currentTimeMillis();

        ReversedLinesDirectReader reversedLinesFileReader = new ReversedLinesDirectReader(fileName);
        byte[] line;
        long lineCount = 0;

        ByteArrTaskBuffer byteArrTaskBuffer = new ByteArrTaskBuffer();
        while ((line = reversedLinesFileReader.readLineBytes()) != null) {
            if (byteArrTaskBuffer.isFull()) {
                transCompMediatorPool.execute(new TransCompMediatorTask(byteArrTaskBuffer));
                byteArrTaskBuffer = new ByteArrTaskBuffer();
            }
            byteArrTaskBuffer.addData(line);
            lineCount += line.length;
        }
        transCompMediatorPool.execute(new TransCompMediatorTask(byteArrTaskBuffer));
        transCompMediatorPool.execute(new Runnable() {
            @Override
            public void run() {
                TransCompMediatorTask.syncConsumeReadyJobs();
            }
        });

        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
        if (Server.logger != null) {
            Server.logger.info("computation time:" + (endTime - startTime));
            Server.logger.info("Byte count: " + lineCount);
            Server.logger.info("current records size:" + inRangeRecord.size());
        }
    }

    public static void JoinComputationThread() {
        // join mediator: must before transform and computation, since the data flow pattern
        transCompMediatorPool.shutdown();
        try {
            transCompMediatorPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Server.logger.info("transCompMediatorPool.awaitTermination error.");
            System.out.println("transCompMediatorPool.awaitTermination error.");
            e.printStackTrace();
        }

        // join transform states
        transformPool.shutdown();
        try {
            transformPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Server.logger.info("transformPool.awaitTermination error.");
            System.out.println("transformPool.awaitTermination error.");
            e.printStackTrace();
        }

        // join computation
        computationPool.shutdown();
        try {
            computationPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Server.logger.info("computationPool.awaitTermination error.");
            System.out.println("computationPool.awaitTermination error.");
            e.printStackTrace();
        }

        // join eval update application
        for (int i = 0; i < EVAL_UPDATE_WORKER_NUM; i++) {
            evalUpdateApplyPools[i].shutdown();
            try {
                evalUpdateApplyPools[i].awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Server.logger.info("evalUpdateApplyPools error. " + i);
                System.out.println("evalUpdateApplyPools error. " + i);
                e.printStackTrace();
            }
        }
    }
}

