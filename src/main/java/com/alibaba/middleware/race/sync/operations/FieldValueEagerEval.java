package com.alibaba.middleware.race.sync.operations;

/**
 * Created by yche on 6/20/17.
 */
public class FieldValueEagerEval implements FieldValueEval {
    byte[] filedValue;

    public FieldValueEagerEval(byte[] filedValue) {
        this.filedValue = filedValue;
    }

    @Override
    public String toString() {
        return new String(filedValue);
    }
}
