package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/18/17.
 */
public class RecordKeyValuePair {
    public final KeyOperation keyOperation;
    public final ValueIndexArrWrapper valueIndexArrWrapper;

    public RecordKeyValuePair(KeyOperation keyOperation, ValueIndexArrWrapper valueIndexArrWrapper) {
        this.keyOperation = keyOperation;
        this.valueIndexArrWrapper = valueIndexArrWrapper;
    }
}
