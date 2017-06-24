package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateLastNameOperation extends UpdateOperation {
    byte lastNameFirstIndex = -1;
    byte lastNameSecondIndex = -1;

    public UpdateLastNameOperation(long relevantKey, byte lastNameFirstIndex, byte lastNameSecondIndex) {
        super(relevantKey);
        this.lastNameFirstIndex = lastNameFirstIndex;
        this.lastNameSecondIndex = lastNameSecondIndex;
    }
}
