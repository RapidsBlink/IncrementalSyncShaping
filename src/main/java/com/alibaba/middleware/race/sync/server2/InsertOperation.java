package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation extends NonDeleteOperation {

    public InsertOperation(long relevantKey) {
        super(relevantKey);
    }

    public void changePK(long pk){
        relevantKey=pk;
    }
}
