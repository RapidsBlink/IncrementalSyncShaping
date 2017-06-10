package com.alibaba.middleware.race.sync.server;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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

    // computation
    private static SequentialRestore sequentialRestore = new SequentialRestore();

    // io and computation sync related
    private final static int fullNum = 5000000;
    private final static int closeEmptyNum = 1000000;
    private final static ReentrantLock isFullLock = new ReentrantLock();
    private final static Condition isFull = isFullLock.newCondition();
    private final static BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(fullNum);
    private final static ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, taskQueue);
    private static boolean isOthersAwakeMe = false;
    private static boolean isDirectReaderSleep = false;

    // intermediate result
    final static Map<Long, RecordUpdate> inRangeActiveKeys = new HashMap<>();
    final static Set<Long> outOfRangeActiveKeys = new HashSet<>();
    final static Set<Long> deadKeys = new HashSet<>();

    // final result
    static ArrayList<String> filedList = new ArrayList<>();
    public final static Map<Long, String> inRangeRecord = new TreeMap<>();

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

    private static class SingleComputationTask implements Runnable {
        private String line;

        SingleComputationTask(String line) {
            this.line = line;
        }

        @Override
        public void run() {
            if (isDirectReaderSleep && taskQueue.size() <= closeEmptyNum) {
                isFullLock.lock();
                if (isDirectReaderSleep) {
                    isOthersAwakeMe = true;
                    isFull.signal();
                }
                isFullLock.unlock();
            }
            sequentialRestore.compute(line);
        }
    }

    public static void OneRound(String fileName) throws IOException {
        long startTime = System.currentTimeMillis();
        ReversedLinesDirectReader reversedLinesFileReader = new ReversedLinesDirectReader(fileName);
        String line;

        while ((line = reversedLinesFileReader.readLine()) != null) {
            if (taskQueue.size() >= fullNum) {
                while (!isOthersAwakeMe) {
                    isFullLock.lock();
                    try {
                        isDirectReaderSleep = true;
                        isFull.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        isFullLock.unlock();
                    }
                }
            }
            isDirectReaderSleep = false;
            isOthersAwakeMe = false;
            pool.execute(new SingleComputationTask(line));
        }

        // update pool states
        pool.shutdown();

        // join threads
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
    }
}

