package com.alibaba.middleware.race.sync.server2.operations;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.inRangeRecordSet;
import static com.alibaba.middleware.race.sync.server2.RestoreComputation.recordMap;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateKeyOperation extends LogOperation {
    public final long changedKey;

    public UpdateKeyOperation(long prevKey, long changedKey) {
        super(prevKey);
        this.changedKey = changedKey;
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) recordMap.get(this); //2
        if (PipelinedComputation.isKeyInRange(this.relevantKey)) {
            inRangeRecordSet.remove(this);
        }

        insertOperation.changePK(this.changedKey); //4
        recordMap.put(insertOperation); //5

        if (PipelinedComputation.isKeyInRange(insertOperation.relevantKey)) {
            inRangeRecordSet.add(insertOperation);
        }
    }
}
