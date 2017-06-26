package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.InsertOperation;

/**
 * Created by yche on 6/23/17.
 */
public class YcheLongObjectHashMap extends YcheLongHash {
    private transient InsertOperation[] _values;

    YcheLongObjectHashMap(int initialCapacity) {
        super(initialCapacity);
        setUp(initialCapacity);
    }

    public void setUp(int initialCapacity) {
        _values = new InsertOperation[initialCapacity];
    }

    public InsertOperation get(long key) {
        int index = index(key);
        return index < 0 ? null : _values[index];
    }

    private void doPut(InsertOperation value, int index) {
        if (index < 0) {
            index = -index - 1;
        }
        _values[index] = value;
    }

    public void put(InsertOperation logOperation) {
        // insertKey() inserts the key if a slot if found and returns the index
        int index = insertKey(logOperation.relevantKey);
        doPut(logOperation, index);
    }
}
