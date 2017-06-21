package com.alibaba.middleware.race.sync.server2;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yche on 6/19/17.
 */
public class UpdateKeyOperation extends UpdateOperation {
    public static AtomicInteger count = new AtomicInteger(0);
    public final long changedKey;

    public UpdateKeyOperation(long prevKey, long changedKey) {
        super(prevKey);
        count.incrementAndGet();
        this.changedKey = changedKey;
    }
}
