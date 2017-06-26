package com.alibaba.middleware.race.sync.server2.operations;


import static com.alibaba.middleware.race.sync.server2.RestoreComputation.recordMap;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateOperation extends NonDeleteOperation {
    public UpdateOperation(long relevantKey) {
        super(relevantKey);
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) recordMap.get(this); //2
        insertOperation.mergeAnother(this); //3
    }
}
