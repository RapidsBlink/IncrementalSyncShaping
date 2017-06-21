package com.alibaba.middleware.race.sync.server2;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yche on 6/19/17.
 */
public class DeleteOperation extends LogOperation {
    public static AtomicInteger count = new AtomicInteger(0);

    public DeleteOperation(long pk) {
        super(pk);
        count.incrementAndGet();
    }
}
