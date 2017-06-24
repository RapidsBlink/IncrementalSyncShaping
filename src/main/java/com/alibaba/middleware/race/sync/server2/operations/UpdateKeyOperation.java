package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateKeyOperation extends LogOperation {
    public final long changedKey;

    public UpdateKeyOperation(long prevKey, long changedKey) {
        super(prevKey);
        this.changedKey = changedKey;
    }
}
