package com.alibaba.middleware.race.sync.server2.operations;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.recordMap;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateScore extends LogOperation {
    private short score = -1;

    public UpdateScore(long relevantKey, short score) {
        super(relevantKey);
        this.score = score;
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) recordMap.get(this); //2
        insertOperation.score = this.score;
    }
}
