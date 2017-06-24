package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateSex extends UpdateOperation {
    byte sexIndex = -1;

    public UpdateSex(long relevantKey, byte sexIndex) {
        super(relevantKey);
        this.sexIndex = sexIndex;
    }
}
