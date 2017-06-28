package com.alibaba.middleware.race.sync.server2.operations;


import com.alibaba.middleware.race.sync.server2.PipelinedComputation;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.ycheArr;

/**
 * Created by yche on 6/19/17.
 */
public class DeleteOperation extends LogOperation {
    public static short[] deleteGlobalIndices = new short[8 * 1024 * 1024];

    static {
        for (int i = 0; i < deleteGlobalIndices.length; i++) {
            deleteGlobalIndices[i] = -1;
        }
    }

    short globalIndex;

    public DeleteOperation(long pk, short globalIndex) {
        super(pk);
        this.globalIndex = globalIndex;
    }

    @Override
    public void act() {
        ycheArr[(int) (this.relevantKey)] = null;
        deleteGlobalIndices[(int) this.relevantKey] = globalIndex;
    }
}
