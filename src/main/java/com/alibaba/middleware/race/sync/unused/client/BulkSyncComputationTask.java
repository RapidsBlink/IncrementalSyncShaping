package com.alibaba.middleware.race.sync.unused.client;

import com.alibaba.middleware.race.sync.unused.server.RecordUpdate;

import java.util.ArrayList;

import static com.alibaba.middleware.race.sync.unused.client.GlobalComputation.*;

/**
 * Created by yche on 6/8/17.
 * also lazy computation
 * computation result directly applied on inRangeRecord(ConcurrentSkipListMap)
 */
public class BulkSyncComputationTask {
    // data
    private final ArrayList<RecordUpdate> inChunkUpdateList;
    private final int lowerIdx; //inclusive
    private final int upperIdx; //exclusive

    public BulkSyncComputationTask(ArrayList<RecordUpdate> inChunkUpdateList, int upperIdx, int lowerIdx) {
        this.inChunkUpdateList = inChunkUpdateList;
        this.upperIdx = upperIdx;
        this.lowerIdx = lowerIdx;
    }

    private RecordUpdate recordUpdate;

    private void actForFirstInsert() {
        if (deadKeys.contains(recordUpdate.lastKey)) {
            // insert-delete
            deadKeys.remove(recordUpdate.lastKey);
        } else if (outOfRangeActiveKeys.contains(recordUpdate.lastKey)) {
            // insert-update out-of-range
            outOfRangeActiveKeys.remove(recordUpdate.lastKey);
        } else if (inRangeActiveKeys.containsKey(recordUpdate.lastKey)) {
            // insert-update in-range
            RecordUpdate prevUpdate = inRangeActiveKeys.get(recordUpdate.lastKey);
            prevUpdate.mergeAnotherIfPossible(recordUpdate);
            inRangeActiveKeys.remove(recordUpdate.lastKey);

            // write string to skip list
            inRangeRecord.put(prevUpdate.lastKey, prevUpdate.toOneLineString(filedList));
        } else {
            // first-time appearing
            if (isKeyInRange(recordUpdate.lastKey)) {
                // write string to skip list
                inRangeRecord.put(recordUpdate.lastKey, recordUpdate.toOneLineString(filedList));
            }
            // else do nothing
        }
    }

    private void actForUpdate() {
        if (deadKeys.contains(recordUpdate.lastKey)) {
            // update-delete
            if (recordUpdate.isKeyChanged()) {
                deadKeys.remove(recordUpdate.lastKey);
                deadKeys.add(recordUpdate.firstKey);
            }
        } else if (outOfRangeActiveKeys.contains(recordUpdate.lastKey)) {
            // update-update out-of-range
            if (recordUpdate.isKeyChanged()) {
                outOfRangeActiveKeys.remove(recordUpdate.lastKey);
                outOfRangeActiveKeys.add(recordUpdate.firstKey);
            }
        } else if (inRangeActiveKeys.containsKey(recordUpdate.lastKey)) {
            // update-update in-range
            RecordUpdate prevUpdate = inRangeActiveKeys.get(recordUpdate.lastKey);
            prevUpdate.mergeAnotherIfPossible(recordUpdate);
            if (recordUpdate.isKeyChanged()) {
                inRangeActiveKeys.remove(recordUpdate.lastKey);
                inRangeActiveKeys.put(recordUpdate.firstKey, prevUpdate);
            }
        } else {
            // first-time appearing
            if (isKeyInRange(recordUpdate.lastKey)) {
                // add to inRangeActiveKeys
                inRangeActiveKeys.put(recordUpdate.firstKey, recordUpdate);
            } else {
                // add to outOfRangeActiveKeys
                outOfRangeActiveKeys.add(recordUpdate.firstKey);
            }
        }
    }

    public void compute() {
        for (int i = lowerIdx; i < upperIdx; i++) {
            recordUpdate = inChunkUpdateList.get(i);
            if (recordUpdate.isLastDeleteInChunk) {
                deadKeys.add(recordUpdate.firstKey);
            } else if (recordUpdate.isFirstInsertInChunk) {
                actForFirstInsert();
            } else {
                // update-update
                actForUpdate();
            }
        }
    }
}
