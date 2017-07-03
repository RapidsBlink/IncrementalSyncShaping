package com.alibaba.middleware.race.sync.server2.operations;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.recordMap;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateLastNameOperation extends LogOperation {
    byte lastNameFirstIndex = -1;
    byte lastNameSecondIndex = -1;

    public UpdateLastNameOperation(long relevantKey, byte lastNameFirstIndex, byte lastNameSecondIndex) {
        super(relevantKey);
        this.lastNameFirstIndex = lastNameFirstIndex;
        this.lastNameSecondIndex = lastNameSecondIndex;
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) recordMap.get(this); //2

        insertOperation.lastNameFirstIndex = this.lastNameFirstIndex;
        insertOperation.lastNameSecondIndex = this.lastNameSecondIndex;
    }
}
