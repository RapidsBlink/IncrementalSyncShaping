package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/18/17.
 */
public class RecordKeyValuePair {
    public final KeyOperation keyOperation;
    public final ValueArrWrapper valueIndexArrWrapper;

    public RecordKeyValuePair(KeyOperation keyOperation, ValueArrWrapper valueIndexArrWrapper) {
        this.keyOperation = keyOperation;
        this.valueIndexArrWrapper = valueIndexArrWrapper;
    }
}
