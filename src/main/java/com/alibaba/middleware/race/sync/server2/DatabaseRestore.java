package com.alibaba.middleware.race.sync.server2;


import com.alibaba.middleware.race.sync.server2.operations.*;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by yche on 6/23/17.
 */
public class DatabaseRestore {
    private static TLongObjectHashMap<LogOperation>[] recordMapArr = new TLongObjectHashMap[PipelinedComputation.RESTORE_SLAVE_NUM];
    private static DatabaseRestore[] databaseRestoreWorker = new DatabaseRestore[PipelinedComputation.RESTORE_SLAVE_NUM];
    private static Queue<Future<?>> futures = new LinkedList<>();

    static {
        for (int i = 0; i < recordMapArr.length; i++) {
            recordMapArr[i] = new TLongObjectHashMap<>(24 * 1024 * 1024 / PipelinedComputation.RESTORE_SLAVE_NUM);
            databaseRestoreWorker[i] = new DatabaseRestore(i);
        }
    }

    private HashSet<LogOperation> deadKeys = new HashSet<>();
    private HashMap<LogOperation, LogOperation> activeKeys = new HashMap<>();
    private ArrayList<LogOperation> insertions = new ArrayList<>();
    private TLongObjectHashMap<LogOperation> recordMap;

    private LogOperation changedToObj = new LogOperation(-1);
    private final int index;

    private DatabaseRestore(int index) {
        this.index = index;
        this.recordMap = recordMapArr[index];
    }

    private boolean isMyJob(LogOperation logOperation) {
        long pk;
        if (logOperation instanceof UpdateKeyOperation) {
            pk = ((UpdateKeyOperation) logOperation).changedKey;
        } else {
            pk = logOperation.relevantKey;
        }
        return pk % PipelinedComputation.RESTORE_SLAVE_NUM == index;
    }

    private void restoreDetail(LogOperation logOperation) {
        if (logOperation instanceof DeleteOperation) {
            if (isMyJob(logOperation)) {
                deadKeys.add(logOperation);
            }
        } else if (logOperation instanceof InsertOperation) {
            if (deadKeys.contains(logOperation)) {
                deadKeys.remove(logOperation);
            } else if (activeKeys.containsKey(logOperation)) {
                NonDeleteOperation lastOperation = ((NonDeleteOperation) activeKeys.remove(logOperation));
                lastOperation.backwardMergePrev((NonDeleteOperation) logOperation);
                insertions.add(lastOperation);
            } else if (isMyJob(logOperation)) {
                insertions.add(logOperation);
            }
        } else if (logOperation instanceof UpdateKeyOperation) {
            // pay attention to changed-to key
            changedToObj.relevantKey = ((UpdateKeyOperation) logOperation).changedKey;
            if (deadKeys.contains(changedToObj)) {
                deadKeys.remove(changedToObj);
                deadKeys.add(logOperation);
            } else if (activeKeys.containsKey(changedToObj)) {
                NonDeleteOperation lastOperation = (NonDeleteOperation) activeKeys.remove(changedToObj);
                lastOperation.backwardMergePrev((NonDeleteOperation) logOperation);
                activeKeys.put(logOperation, lastOperation);
            } else if (isMyJob(logOperation)) {
                activeKeys.put(logOperation, logOperation);
            }
        } else {
            // update property
            if (activeKeys.containsKey(logOperation)) {
                ((NonDeleteOperation) activeKeys.get(logOperation)).backwardMergePrev((NonDeleteOperation) logOperation);
            } else if (isMyJob(logOperation)) {
                activeKeys.put(logOperation, logOperation);
            }
        }
    }

    private LogOperation lookUp(LogOperation logOperation) {
        long pk = logOperation.relevantKey;
        int lookUpIndex = (int) (pk % PipelinedComputation.RESTORE_SLAVE_NUM);
        return recordMapArr[lookUpIndex].get(logOperation.relevantKey);
    }

    private void restoreFirstPhase(LogOperation[] logOperations) {
        deadKeys.clear();
        activeKeys.clear();
        insertions.clear();
        for (int i = logOperations.length - 1; i >= 0; i--) {
            restoreDetail(logOperations[i]);
        }

        for (Map.Entry<LogOperation, LogOperation> entry : activeKeys.entrySet()) {
            LogOperation lastOperation = entry.getValue();
            LogOperation prevOperation = entry.getKey();
            ((NonDeleteOperation) lastOperation).backwardMergePrev((NonDeleteOperation) lookUp(prevOperation));
            insertions.add(lastOperation);
        }
    }

    private void restoreApplyPhase() {
        for (LogOperation logOperation : insertions) {
            if (logOperation instanceof UpdateKeyOperation) {
                logOperation.relevantKey = ((UpdateKeyOperation) logOperation).changedKey;
            }
            recordMap.put(logOperation.relevantKey, logOperation);
        }
    }

    static void submitFirstPhase(final LogOperation[] logOperations) {
        for (int i = 0; i < PipelinedComputation.RESTORE_SLAVE_NUM; i++) {
            final int finalI = i;
            futures.add(PipelinedComputation.computationSlaverPools[i].submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            DatabaseRestore.databaseRestoreWorker[finalI].restoreFirstPhase(logOperations);
                        }
                    }
            ));
        }
    }

    private static void condWait() {
        while (!futures.isEmpty()) {
            try {
                futures.poll().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    static void submitSecondPhase() {
        condWait();
        for (int i = 0; i < PipelinedComputation.RESTORE_SLAVE_NUM; i++) {
            final int finalI = i;
            futures.add(PipelinedComputation.computationSlaverPools[i].submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            DatabaseRestore.databaseRestoreWorker[finalI].restoreApplyPhase();
                        }
                    }
            ));
        }
    }
}
