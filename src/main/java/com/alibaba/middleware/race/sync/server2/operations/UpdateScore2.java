package com.alibaba.middleware.race.sync.server2.operations;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.recordMap;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateScore2 extends LogOperation {
    int score2 = -1;

    public UpdateScore2(long relevantKey, int score2) {
        super(relevantKey);
        this.score2 = score2;
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) recordMap.get(this); //2
        insertOperation.score2 = this.score2;
    }
}
