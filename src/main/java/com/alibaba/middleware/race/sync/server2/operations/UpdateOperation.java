package com.alibaba.middleware.race.sync.server2.operations;

import com.alibaba.middleware.race.sync.server2.RestoreComputation;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateOperation extends NonDeleteOperation {
    public UpdateOperation(long relevantKey) {
        super(relevantKey);
    }

    @Override
    public void act() {
        InsertOperation insertOperation = (InsertOperation) RestoreComputation.ycheArr[(int) (this.relevantKey)]; //2
        if(insertOperation==null){
            insertOperation=new InsertOperation(this.relevantKey);
            RestoreComputation.ycheArr[(int) this.relevantKey]=insertOperation;
        }
        insertOperation.mergeAnother(this); //3
    }
}
