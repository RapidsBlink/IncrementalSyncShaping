package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateFirstNameOperation extends LogOperation {
    byte firstNameIndex = -1;

    public UpdateFirstNameOperation(long relevantKey, byte firstNameIndex) {
        super(relevantKey);
        this.firstNameIndex = firstNameIndex;
    }
}
