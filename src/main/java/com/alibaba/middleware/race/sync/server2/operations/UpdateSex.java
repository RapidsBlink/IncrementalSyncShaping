package com.alibaba.middleware.race.sync.server2.operations;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.recordMap;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateSex extends LogOperation {
    private byte sexIndex = -1;

    public UpdateSex(long relevantKey, byte sexIndex) {
        super(relevantKey);
        this.sexIndex = sexIndex;
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) recordMap.get(this); //2
        insertOperation.sexIndex = this.sexIndex;
    }
}
