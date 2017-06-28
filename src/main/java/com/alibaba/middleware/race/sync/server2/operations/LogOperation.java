package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/19/17.
 */
public abstract class LogOperation  {
    public long relevantKey;

    public LogOperation(long relevantKey) {
        this.relevantKey = relevantKey;
    }

    public abstract void act();
}
