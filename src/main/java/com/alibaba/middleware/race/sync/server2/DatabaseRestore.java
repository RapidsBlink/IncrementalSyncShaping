package com.alibaba.middleware.race.sync.server2;


import com.alibaba.middleware.race.sync.server2.operations.*;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by yche on 6/23/17.
 */
class DatabaseRestore {
    private static TLongObjectHashMap<LogOperation>[] recordMapArr = new TLongObjectHashMap[PipelinedComputation.RESTORE_SLAVE_NUM];
    private static DatabaseRestore[] databaseRestoreWorker = new DatabaseRestore[PipelinedComputation.RESTORE_SLAVE_NUM];
    private static Queue<Future<?>> futures = new LinkedList<>();

    static {
        for (int i = 0; i < recordMapArr.length; i++) {
            recordMapArr[i] = new TLongObjectHashMap<>(24 * 1024 * 1024 / PipelinedComputation.RESTORE_SLAVE_NUM);
            databaseRestoreWorker[i] = new DatabaseRestore(i);
        }
    }

    // 1st: dead keys
    private TLongSet deadKeys = new TLongHashSet();
    // 2nd: active keys, key: in chunk begin, value: in chunk end
    private TLongObjectHashMap<LogOperation> activeKeys = new TLongObjectHashMap<>();

    private ArrayList<LogOperation> insertions = new ArrayList<>();
    private TLongObjectHashMap<LogOperation> recordMap;

    private final int index;

    private DatabaseRestore(int index) {
        this.index = index;
        this.recordMap = recordMapArr[index];
    }

    // only collect 2 type of works, first: change to my duty, insert/update applied to my duty
    private boolean isMyJob(LogOperation logOperation) {
        long pk;
        if (logOperation instanceof UpdateKeyOperation) {
            pk = ((UpdateKeyOperation) logOperation).changedKey;
        } else {
            pk = logOperation.relevantKey;
        }
        return pk % PipelinedComputation.RESTORE_SLAVE_NUM == index;
    }

    private static long debugKey = 259479554;

    private void restoreDetail(LogOperation logOperation) {
        long relevantKey = logOperation.relevantKey;
        if (relevantKey == debugKey) {
            System.out.println("debug relevant key");
        }
        if (logOperation instanceof DeleteOperation) {
            deadKeys.add(relevantKey);
        } else if (logOperation instanceof InsertOperation) {
            if (deadKeys.contains(relevantKey)) {
                deadKeys.remove(relevantKey);
            } else if (activeKeys.containsKey(relevantKey)) {
                NonDeleteOperation lastOperation = ((NonDeleteOperation) activeKeys.remove(relevantKey));
                lastOperation.backwardMergePrev((NonDeleteOperation) logOperation);
                if (isMyJob(lastOperation))
                    insertions.add(lastOperation);
            } else if (isMyJob(logOperation)) {
                insertions.add(logOperation);
            }
        } else if (logOperation instanceof UpdateKeyOperation) {
            long changedKey = ((UpdateKeyOperation) logOperation).changedKey;
            if (changedKey == debugKey) {
                System.out.println("debug changed key");
            }

            // pay attention to changed-to key
            if (deadKeys.contains(changedKey)) {
                deadKeys.remove(changedKey);
                deadKeys.add(relevantKey);
                if (changedKey == debugKey) {
                    System.out.println("debug changed key dead keys");
                }
            } else if (activeKeys.containsKey(changedKey)) {
                NonDeleteOperation lastOperation = (NonDeleteOperation) activeKeys.remove(changedKey);
                activeKeys.put(relevantKey, lastOperation);
                if (changedKey == debugKey) {
                    System.out.println("debug changed key active keys");
                }
            } else {
                activeKeys.put(relevantKey, new UpdateOperation(changedKey));
                if (changedKey == debugKey) {
                    System.out.println("debug changed key last operation");
                }
            }
        } else {
            // update property, if in deadKeys, do nothing
            if (activeKeys.containsKey(relevantKey)) {
                ((NonDeleteOperation) activeKeys.get(relevantKey)).backwardMergePrev((NonDeleteOperation) logOperation);
            } else {
                activeKeys.put(relevantKey, logOperation);
            }
        }
    }

    private NonDeleteOperation lookUp(long pk) {
        int lookUpIndex = (int) (pk % PipelinedComputation.RESTORE_SLAVE_NUM);
        if (recordMapArr[lookUpIndex].get(pk) == null) {
            System.out.println(lookUpIndex + " , null for relevant key:" + pk);
        }
        return (NonDeleteOperation) recordMapArr[lookUpIndex].get(pk);
    }

    private void restoreFirstPhase(LogOperation[] logOperations) {
        // 1st: clear status
        deadKeys.clear();
        activeKeys.clear();
        insertions.clear();

        // 2nd: reverse update status
        for (int i = logOperations.length - 1; i >= 0; i--) {
            restoreDetail(logOperations[i]);
        }

        // 3rd: fetch possible previous properties
        activeKeys.forEachEntry(new TLongObjectProcedure<LogOperation>() {
            @Override
            public boolean execute(long key, LogOperation lastOperation) {
                if (isMyJob(lastOperation)) {
                    ((NonDeleteOperation) lastOperation).backwardMergePrev(lookUp(key));
                    insertions.add(lastOperation);
                }
                return true;
            }
        });
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
        condWait();
    }

    // bsp barrier: for the first phase comp
    private static void condWait() {
        while (!futures.isEmpty()) {
            try {
                futures.poll().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
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

    static void submitSecondPhase() {
        for (int i = 0; i < PipelinedComputation.RESTORE_SLAVE_NUM; i++) {
            final int finalI = i;
            futures.add(PipelinedComputation.computationSlaverPools[i].submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            databaseRestoreWorker[finalI].restoreApplyPhase();
                        }
                    }
            ));
        }
        condWait();
    }

    static LogOperation getLogOperation(long pk) {
        int index = (int) (pk % PipelinedComputation.RESTORE_SLAVE_NUM);
        return recordMapArr[index].get(pk);
    }
}
