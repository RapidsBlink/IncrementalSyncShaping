package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.Constants.LINE_SPLITTER;
import static com.alibaba.middleware.race.sync.server2.FileUtil.unmap;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.*;

/**
 * Created by yche on 6/16/17.
 * used by the master thread
 */
class FileTransformMediatorTask {
    private Queue<Future<RecordScanner>> prevFutureQueue = new LinkedList<>();
    private Queue<Future<?>> computationFutures = new LinkedList<>();
    private ArrayList<RecordScanner> recordScanners = new ArrayList<>();

    private MappedByteBuffer mappedByteBuffer;
    private int currChunkLength;
    boolean isFinished = false;

    FileTransformMediatorTask() {
        isFinished = true;
    }

    FileTransformMediatorTask(MappedByteBuffer mappedByteBuffer, int currChunkLength) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.currChunkLength = currChunkLength;
    }

    private static ByteBuffer prevRemainingBytes = ByteBuffer.allocate(32 * 1024);

    // previous tail, should be copied into task
    private int preparePrevBytes() {
        int end = 0;

        if (prevRemainingBytes.position() > 0 && prevRemainingBytes.get(prevRemainingBytes.position() - 1) != LINE_SPLITTER) {
            byte myByte;
            // stop at `\n`
            while ((myByte = mappedByteBuffer.get(end)) != LINE_SPLITTER) {
                prevRemainingBytes.put(myByte);
                end++;
            }
            prevRemainingBytes.put(myByte);
            end++;
        }
        prevRemainingBytes.flip();
        return end;
    }

    private int computeEnd(int smallChunkLastIndex) {
        int end = smallChunkLastIndex;
        while (mappedByteBuffer.get(end) != LINE_SPLITTER) {
            end--;
        }
        end += 1;
        return end;
    }

    // 2nd work: mergeAnother remaining, compute [start, end)
    private void assignTransformTasks() {
        int avgTask = currChunkLength / fileTransformPool.length;

        // index pair
        int start;
        int end = preparePrevBytes();

        // 1st: first worker
        start = end;
        end = computeEnd(avgTask - 1);
        FileTransformTask fileTransformTask;
        if (prevRemainingBytes.limit() > 0) {
            ByteBuffer tmp = ByteBuffer.allocate(prevRemainingBytes.limit());
            tmp.put(prevRemainingBytes);
            fileTransformTask = new FileTransformTask(mappedByteBuffer, start, end, tmp);
        } else {
            fileTransformTask = new FileTransformTask(mappedByteBuffer, start, end);
        }

        prevFutureQueue.add(fileTransformPool[0].submit(fileTransformTask));

        // 2nd: subsequent workers
        for (int i = 1; i < fileTransformPool.length; i++) {
            start = end;
            int smallChunkLastIndex = i < fileTransformPool.length - 1 ? avgTask * (i + 1) - 1 : currChunkLength - 1;
            end = computeEnd(smallChunkLastIndex);
            fileTransformTask = new FileTransformTask(mappedByteBuffer, start, end);
            prevFutureQueue.add(fileTransformPool[i].submit(fileTransformTask));
        }

        // current tail, reuse and then put
        prevRemainingBytes.clear();
        for (int i = end; i < currChunkLength; i++) {
            prevRemainingBytes.put(mappedByteBuffer.get(i));
        }
    }

    void transform() {
        assignTransformTasks();
        while (!prevFutureQueue.isEmpty()) {
            try {
                recordScanners.add(prevFutureQueue.poll().get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // hash set
        computationFutures.add(computationPool.submit(new Runnable() {
            @Override
            public void run() {
                for (RecordScanner recordScanner : recordScanners) {
                    RestoreComputation.compute(recordScanner.localOperations);
                }
            }
        }));
        // hash map
        for (int i = 0; i < RestoreComputation.WORKER_NUM; i++) {
            final int finalI = i;
            computationFutures.add(dbUpdatePool[i].submit(new Runnable() {
                @Override
                public void run() {
                    for (RecordScanner recordScanner : recordScanners) {
                        RestoreComputation.computeDatabase(recordScanner.workerOperations[finalI], finalI);
                    }
                }
            }));
        }

        unmap(mappedByteBuffer);
        while (!computationFutures.isEmpty()) {
            try {
                computationFutures.poll().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

}
