package com.alibaba.middleware.race.sync.server2;

/**
 * Created by yche on 6/18/17.
 */
class RecordKeyValuePair {
    final KeyOperation keyOperation;
    final ValueIndexArrWrapper valueIndexArrWrapper;

    RecordKeyValuePair(KeyOperation keyOperation, ValueIndexArrWrapper valueIndexArrWrapper) {
        this.keyOperation = keyOperation;
        this.valueIndexArrWrapper = valueIndexArrWrapper;
    }
}
