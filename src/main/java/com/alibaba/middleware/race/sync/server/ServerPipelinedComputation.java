package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.Server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private static TaskBuffer taskBuffer = new TaskBuffer();
    private final static ArrayList<String> fileList = new ArrayList<>();

    public interface FindResultListener {
        void sendToClient(String result);
    }

    public static void readFilesIntoPageCache(ArrayList<String> fileList) throws IOException {
        long startTime = System.currentTimeMillis();

        String line;
        long byteCount = 0L;
        long lineCount = 0L;
        for (String filePath : fileList) {
            BufferedReader fileReader = new BufferedReader(new FileReader(filePath));
            while ((line = fileReader.readLine()) != null) {
                byteCount += line.length();
                lineCount++;
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println("read files into page cache, cost: " + (endTime - startTime) + "ms , byte count:" + byteCount + " , line count:" + lineCount);
        if (Server.logger != null) {
            Server.logger.info("read files into page cache, cost: " + (endTime - startTime) + "ms , byte count:" + byteCount + " , line count:" + lineCount);
        }
    }

    private static class TaskBuffer {
        static int MAX_SIZE = 100000;
        String[] stringArr = new String[MAX_SIZE];    // 1MB
        private int nextIndex = 0;

        private void addData(String line) {
            stringArr[nextIndex] = line;
            nextIndex++;
        }

        public boolean isFull() {
            return nextIndex >= MAX_SIZE;
        }

        public int length() {
            return nextIndex;
        }

        public String get(int idx) {
            return stringArr[idx];
        }
    }

    private static class SingleComputationTask implements Runnable {
        private TaskBuffer taskBuffer;
        private FindResultListener findResultListener;

        SingleComputationTask(TaskBuffer taskBuffer, FindResultListener findResultListener) {
            this.taskBuffer = taskBuffer;
            this.findResultListener = findResultListener;
        }

        @Override
        public void run() {
            for (int i = 0; i < taskBuffer.length(); i++) {
                String result = sequentialRestore.compute(taskBuffer.get(i));
                if (result != null) {
                    findResultListener.sendToClient(result);
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
            if (taskBuffer.isFull()) {
                computationPool.execute(new SingleComputationTask(taskBuffer, findResultListener));
                taskBuffer = new TaskBuffer();
            }
            taskBuffer.addData(line);
            lineCount += line.length();
        }
        computationPool.execute(new SingleComputationTask(taskBuffer, findResultListener));

        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
        if (Server.logger != null) {
            Server.logger.info("computation time:" + (endTime - startTime));
            Server.logger.info("Byte count: " + lineCount);
        }
    }

    public static void JoinComputationThread() {
        // update computationPool states
        computationPool.shutdown();
       // pageCachePool.shutdown();
        // join threads
        try {
            computationPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
           // pageCachePool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

