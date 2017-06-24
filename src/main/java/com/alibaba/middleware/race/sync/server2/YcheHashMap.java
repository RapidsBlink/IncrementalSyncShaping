package com.alibaba.middleware.race.sync.server2;

import gnu.trove.impl.hash.TLongHash;

import java.util.Arrays;

/**
 * Created by yche on 6/23/17.
 */
public class YcheHashMap extends TLongHash {
    protected transient LogOperation[] _values;

    public YcheHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public int setUp(int initialCapacity) {
        int capacity;

        capacity = super.setUp(initialCapacity);
        //noinspection unchecked
        _values = new LogOperation[capacity];
        return capacity;
    }

    @Override
    protected void rehash(int newCapacity) {

    }

    public LogOperation get(Object key) {
        int index = index(((LogOperation)key).relevantKey);
        return index < 0 ? null : _values[index];
    }

    private void doPut(LogOperation value, int index) {
        boolean isNewMapping = true;
        if (index < 0) {
            index = -index - 1;
            isNewMapping = false;
        }
        _values[index] = value;
        if (isNewMapping) {
            postInsertHook(consumeFreeSlot);
        }

    }

    public void put(LogOperation key) {
        // insertKey() inserts the key if a slot if found and returns the index
        int index = insertKey(key.relevantKey);
        doPut(key, index);
    }
}
