package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.LogOperation;

/**
 * Created by yche on 6/23/17.
 */
public class YcheHashMap extends YcheLongHash {
    private transient LogOperation[] _values;

    YcheHashMap(int initialCapacity) {
        super(initialCapacity);
        setUp(initialCapacity);
    }

    public void setUp(int initialCapacity) {
        _values = new LogOperation[initialCapacity];
    }

    public LogOperation get(Object key) {
        int index = index(((LogOperation)key).relevantKey);
        return index < 0 ? null : _values[index];
    }

    private void doPut(LogOperation value, int index) {
        if (index < 0) {
            index = -index - 1;
        }
        _values[index] = value;
    }

    public void put(LogOperation key) {
        // insertKey() inserts the key if a slot if found and returns the index
        int index = insertKey(key.relevantKey);
        doPut(key, index);
    }
}
