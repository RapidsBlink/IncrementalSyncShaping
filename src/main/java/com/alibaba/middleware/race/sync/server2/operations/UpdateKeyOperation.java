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
}
