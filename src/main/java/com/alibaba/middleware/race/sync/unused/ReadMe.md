
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

## Condition Variable Version Pipelined Computation

```java
package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.Server;

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

    // computation
    private static SequentialRestore sequentialRestore = new SequentialRestore();

    private final static ExecutorService pool = Executors.newSingleThreadExecutor();

    // io and computation sync related
//    private final static int fullNum = 5000000;
//    private final static int closeEmptyNum = 2000000;
//    private final static ReentrantLock isFullLock = new ReentrantLock();
//    private final static Condition isFull = isFullLock.newCondition();
//    private final static BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(fullNum);
//    private final static ExecutorService pool = new ThreadPoolExecutor(1, 1,
//            0L, TimeUnit.MILLISECONDS, taskQueue);
//    private static boolean isOthersAwakeMe = false;
//    private static boolean isDirectReaderSleep = false;

    // intermediate result
    final static Map<Long, RecordUpdate> inRangeActiveKeys = new HashMap<>();
    final static Set<Long> outOfRangeActiveKeys = new HashSet<>();
    final static Set<Long> deadKeys = new HashSet<>();

    // final result
    static ArrayList<String> filedList = new ArrayList<>();
    public final static Map<Long, String> inRangeRecord = new TreeMap<>();

    public static interface FindResultListener {
        public void sendToClient(String result);
    }

    private static class SingleComputationTask implements Runnable {
        private String line;
        private FindResultListener findResultListener;

        SingleComputationTask(String line, FindResultListener findResultListener) {
            this.line = line;
            this.findResultListener = findResultListener;
        }

        @Override
        public void run() {
//            if (isDirectReaderSleep && taskQueue.size() <= closeEmptyNum) {
//                isFullLock.lock();
//                if (isDirectReaderSleep) {
//                    isOthersAwakeMe = true;
//                    isFull.signal();
//                }
//                isFullLock.unlock();
//            }
            String result = sequentialRestore.compute(line);
            if (result != null) {
                findResultListener.sendToClient(result);
            }
        }
    }

    public static void OneRoundComputation(String fileName, FindResultListener findResultListener) throws IOException {
        long startTime = System.currentTimeMillis();
        ReversedLinesDirectReader reversedLinesFileReader = new ReversedLinesDirectReader(fileName);
        String line;
        long lineCount = 0;
        while ((line = reversedLinesFileReader.readLine()) != null) {
//            if (taskQueue.size() >= fullNum) {
//                while (!isOthersAwakeMe) {
//                    isFullLock.lock();
//                    try {
//                        isDirectReaderSleep = true;
//                        isFull.await();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    } finally {
//                        isFullLock.unlock();
//                    }
//                }
//            }
//            isDirectReaderSleep = false;
//            isOthersAwakeMe = false;
            pool.execute(new SingleComputationTask(line, findResultListener));

            lineCount += line.length();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
        if (Server.logger != null) {
            Server.logger.info("computation time:" + (endTime - startTime));
            Server.logger.info("Byte count: " + lineCount);
        }
    }

    public static void JoinComputationThread() {
        // update pool states
        pool.shutdown();

        // join threads
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}



```


```java
package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.server2.FileTransformWriteMediator;
import com.alibaba.middleware.race.sync.unused.LineDirectReader;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yche on 6/11/17.
 */
final public class FileUtil {
    public static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    static void readFileIntoPageCache(String filePath) throws IOException {
        File file = new File(filePath);
        int fileSize = (int) file.length();

        int CHUNK_SIZE = 64 * 1024 * 1024;
        int maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        int lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;
        FileChannel fileChannel = new RandomAccessFile(filePath, "r").getChannel();
        MappedByteBuffer mappedByteBuffer;
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, maxIndex * CHUNK_SIZE, lastChunkLength);
        mappedByteBuffer.load();
        unmap(mappedByteBuffer);
        for (int i = maxIndex - 1; i >= 0; i--) {
            //System.out.println(i);
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i * CHUNK_SIZE, CHUNK_SIZE);
            mappedByteBuffer.load();
            unmap(mappedByteBuffer);
        }
        //System.out.println(maxIndex);

    }

    private static void transformFiles(String fileName) throws IOException {
        FileTransformWriteMediator fileTransformWriteMediator = new FileTransformWriteMediator(fileName, "/tmp", "/home/yche/OutData");
        fileTransformWriteMediator.transformFile();
    }

    public static void transferFile(String fileName, String srcFolder, String dstFolder) throws IOException {
        FileChannel srcFileChannel = new RandomAccessFile(srcFolder + File.separator + fileName, "r").getChannel();
        FileChannel dstFileChannel = new RandomAccessFile(dstFolder + File.separator + fileName, "rw").getChannel();

        dstFileChannel.transferFrom(srcFileChannel, 0, srcFileChannel.size());

        srcFileChannel.close();
        dstFileChannel.close();

    }

    static int CHUNK_SIZE = 64 * 1024 * 1024;
    private static byte[] internalBuff = new byte[CHUNK_SIZE];
    private static byte[] reducedBuff = new byte[CHUNK_SIZE];
    private static int reduceBuffIndex = 0;

    public static void copyFiles(String fileName, String srcFolder, String dstFolder) throws IOException {
        FileChannel srcFileChannel = new RandomAccessFile(srcFolder + File.separator + fileName, "r").getChannel();
        File file = new File(srcFolder + File.separator + fileName);
        long fileSize = file.length();

        //LineDirectReader lineDirectReader = new LineDirectReader(srcFolder + File.separator + fileName);

        MappedFileWriter mappedFileWriter = new MappedFileWriter(dstFolder + File.separator + fileName, fileSize);

//        byte[] line;
//        while((line = lineDirectReader.readLineBytes()) != null){
//            mappedFileWriter.write(line);
//        }

        MappedByteBuffer srcMappedByteBuffer = null;

        long maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        long lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;
        reduceBuffIndex = 0;
        int splitCount = 0;
        for (long nextIndex = 0; nextIndex < maxIndex; nextIndex++) {
            srcMappedByteBuffer = srcFileChannel.map(FileChannel.MapMode.READ_ONLY, nextIndex * CHUNK_SIZE, CHUNK_SIZE);
            srcMappedByteBuffer.load();
            srcMappedByteBuffer.get(internalBuff);

            for (int i = 0; i < CHUNK_SIZE; i++) {
                if (splitCount < 5 && internalBuff[i] != Constants.SPLIT_CHAR) {
                    continue;
                }
                if (splitCount < 5 && internalBuff[i] == Constants.SPLIT_CHAR) {
                    splitCount++;
                    if (splitCount == 5)
                        reducedBuff[reduceBuffIndex++] = '|';
                } else if (internalBuff[i] == '\n') {
                    reducedBuff[reduceBuffIndex++] = '\n';
                    splitCount = 0;
                } else if (splitCount == 5) {
                    reducedBuff[reduceBuffIndex++] = internalBuff[i];
                }
                if (reduceBuffIndex == CHUNK_SIZE) {
                    mappedFileWriter.write(reducedBuff);
                    reduceBuffIndex = 0;
                }
            }
            unmap(srcMappedByteBuffer);

        }
        srcMappedByteBuffer = srcFileChannel.map(FileChannel.MapMode.READ_ONLY, maxIndex * CHUNK_SIZE, lastChunkLength);
        srcMappedByteBuffer.load();
        srcMappedByteBuffer.get(internalBuff, 0, srcMappedByteBuffer.limit());

        for (int i = 0; i < lastChunkLength; i++) {
            if (splitCount < 5 && internalBuff[i] != Constants.SPLIT_CHAR) {
                continue;
            }
            if (splitCount < 5 && internalBuff[i] == Constants.SPLIT_CHAR) {
                splitCount++;
                if (splitCount == 5)
                    reducedBuff[reduceBuffIndex++] = '|';
            } else if (internalBuff[i] == '\n') {
                reducedBuff[reduceBuffIndex++] = '\n';
                splitCount = 0;
            } else if (splitCount == 5) {
                reducedBuff[reduceBuffIndex++] = internalBuff[i];
            }
            if (reduceBuffIndex == CHUNK_SIZE) {
                mappedFileWriter.write(reducedBuff);
                reduceBuffIndex = 0;
            }
        }
        if (reduceBuffIndex > 0)
            mappedFileWriter.write(reducedBuff, 0, reduceBuffIndex);

        unmap(srcMappedByteBuffer);
        mappedFileWriter.close();
    }
}

```