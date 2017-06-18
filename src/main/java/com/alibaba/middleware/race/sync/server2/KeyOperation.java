package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/18/17.
 * record key-change
 */
public class KeyOperation {
    public byte operationType;
    public Long prevKey;
    public Long curKey;

    KeyOperation(byte operationType) {
        this.operationType = operationType;
        this.prevKey = null;
        this.curKey = null;
    }

    void preKey(long prevKey) {
        this.prevKey = prevKey;
    }

    void curKey(long curKey) {
        this.curKey = curKey;
    }

    byte getOperationType() {
        return operationType;
    }

    Long getPrevKey() {
        return prevKey;
    }

    Long getCurKey() {
        return curKey;
    }

    boolean isKeyChanged() {
        return !getPrevKey().equals(getCurKey());
    }

    private static long pkLowerBound;
    private static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
    }

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

}
