package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/18/17.
 */
public class RecordWrapper {
    final KeyOperation keyOperation;
    final ValueIndexArrWrapper valueIndexArrWrapper;

    public RecordWrapper(KeyOperation keyOperation, ValueIndexArrWrapper valueIndexArrWrapper) {
        this.keyOperation = keyOperation;
        this.valueIndexArrWrapper = valueIndexArrWrapper;
    }
}
