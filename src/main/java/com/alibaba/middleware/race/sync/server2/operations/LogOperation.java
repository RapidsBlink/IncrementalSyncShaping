package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/19/17.
 */
public abstract class LogOperation implements Comparable<LogOperation> {
    private static int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public long relevantKey;

    LogOperation(long relevantKey) {
        this.relevantKey = relevantKey;
    }

    @Override
    public int hashCode() {
        return (int) (relevantKey ^ (relevantKey >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LogOperation && relevantKey == ((LogOperation) obj).relevantKey;
    }

    @Override
    public int compareTo(LogOperation o) {
        return compare(relevantKey, o.relevantKey);
    }
}
