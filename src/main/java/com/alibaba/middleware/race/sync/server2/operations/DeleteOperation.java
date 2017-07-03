package com.alibaba.middleware.race.sync.server2.operations;


import com.alibaba.middleware.race.sync.server2.PipelinedComputation;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.inRangeRecordSet;

/**
 * Created by yche on 6/19/17.
 */
public class DeleteOperation extends LogOperation {
    public DeleteOperation(long pk) {
        super(pk);
    }

    @Override
    public void act() {
        if (PipelinedComputation.isKeyInRange(this.relevantKey)) {
            inRangeRecordSet.remove(this);
        }
    }
}
