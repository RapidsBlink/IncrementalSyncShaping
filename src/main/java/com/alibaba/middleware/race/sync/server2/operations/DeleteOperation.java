package com.alibaba.middleware.race.sync.server2.operations;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.ycheArr;

/**
 * Created by yche on 6/19/17.
 */
public class DeleteOperation extends LogOperation {
    public DeleteOperation(long pk) {
        super(pk);
    }

    @Override
    public void act() {
        ycheArr[(int) (this.relevantKey)] = null;
    }
}
