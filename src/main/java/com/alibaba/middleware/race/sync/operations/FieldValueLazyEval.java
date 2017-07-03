package com.alibaba.middleware.race.sync.operations;

import com.alibaba.middleware.race.sync.server2.RestoreComputation;

/**
 * Created by yche on 6/20/17.
 */
public class FieldValueLazyEval implements FieldValueEval {
    final long pk;
    final byte filedIndex;

    public FieldValueLazyEval(long pk, byte filedIndex) {
        this.pk = pk;
        this.filedIndex = filedIndex;
    }

    public FieldValueEval eval() {
        return RestoreComputation.currentDB.get(pk).filedValuePointers[filedIndex];
    }
}
