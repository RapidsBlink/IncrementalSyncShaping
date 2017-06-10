## Pre-processing

clear page cache

```zsh
echo 1 > /proc/sys/vm/drop_caches
```

read file

```zsh
all line num:14787781
filtered line num:14787781
file bytes:1617855426
average byte per log:109.404884
read time:19706 ms
```

## Parallel(Not Useful Overlap)

```java
package com.alibaba.middleware.race.sync.play;

import com.alibaba.middleware.race.sync.server.RecordUpdate;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 6/8/17.
 */
public class GlobalComputation {
    private final static ConcurrentLinkedDeque<String> toEvalStringList = new ConcurrentLinkedDeque<>();
    private static boolean isAnyEvalStringToBeAdded = true;
    private static boolean isOtherAwakeReader = false;
    private static boolean isSleep = false;

    private final static ConsumerThread consumerThread = new ConsumerThread();

    private final static int fullNum = 3000000;
    private final static int closeEmptyNum = 1200000;
    private final static ReentrantLock isFullLock = new ReentrantLock();
    private final static Condition isFull = isFullLock.newCondition();

    final static Map<Long, RecordUpdate> inRangeActiveKeys = new HashMap<>();
    final static Set<Long> outOfRangeActiveKeys = new HashSet<>();
    final static Set<Long> deadKeys = new HashSet<>();

    static ArrayList<String> filedList = new ArrayList<>();
    public final static Map<Long, String> inRangeRecord = new TreeMap<>();

    private static long pkLowerBound;
    private static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
    }

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

    public static long extractPK(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\t') {
                break;
            }
            sb.append(str.charAt(i));
        }
        return Long.parseLong(sb.toString());
    }

    public static void produceTask(String evalString) {
//        if (toEvalStringList.size() >= fullNum) {
//            while (!isOtherAwakeReader) {
//                isFullLock.lock();
//                try {
//                    isSleep = true;
//                    isFull.await();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } finally {
//                    isFullLock.unlock();
//                }
//            }
//            isOtherAwakeReader = false;
//            isSleep = false;
//        }
        toEvalStringList.add(evalString);
    }

    public static void finishProduce() {
        isAnyEvalStringToBeAdded = false;
    }

    private static class ConsumerThread extends Thread {
        SequentialRestore sequentialRestore = new SequentialRestore();

        @Override
        public void run() {
            // busy waiting
            while (isAnyEvalStringToBeAdded) {
//                if (isSleep && toEvalStringList.size() < closeEmptyNum) {
//                    isFullLock.lock();
//                    isOtherAwakeReader=true;
//                    isFull.signal();
//                    isFullLock.unlock();
//                }
//                System.out.println(toEvalStringList.size());
                if (toEvalStringList.size() > 0)
                    sequentialRestore.compute(toEvalStringList.poll());
            }

            // compute the last few tasks
            while (toEvalStringList.size() > 0) {
                sequentialRestore.compute(toEvalStringList.poll());
            }
        }
    }

    public static void startConsumerThread() {
        consumerThread.start();
    }

    public static void joinConsumerThread() {
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


```