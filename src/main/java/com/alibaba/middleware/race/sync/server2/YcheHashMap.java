package com.alibaba.middleware.race.sync.server2;

import gnu.trove.impl.hash.TObjectHash;

import java.util.Arrays;

/**
 * Created by yche on 6/23/17.
 */
public class YcheHashMap extends TObjectHash<LogOperation> {
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
        int oldCapacity = _set.length;
        int oldSize = size();
        Object oldKeys[] = _set;
        LogOperation oldVals[] = _values;

        _set = new Object[newCapacity];
        Arrays.fill(_set, FREE);
        _values = new LogOperation[newCapacity];

        // Process entries from the old array, skipping free and removed slots. Put the
        // values into the appropriate place in the new array.
        int count = 0;
        for (int i = oldCapacity; i-- > 0; ) {
            Object o = oldKeys[i];

            if (o == FREE || o == REMOVED) continue;

            int index = insertKey((LogOperation) o);
            if (index < 0) {
                throwObjectContractViolation(_set[(-index - 1)], o, size(), oldSize, oldKeys);
            }
            _values[index] = oldVals[i];
            //
            count++;
        }

        // Last check: size before and after should be the same
        reportPotentialConcurrentMod(size(), oldSize);
    }

    public LogOperation get(Object key) {
        int index = index(key);
        return index < 0 ? null : _values[index];
    }

    public void remove(Object key) {
        int index = index(key);
        if (index >= 0) {
            removeAt(index);    // clear key,state; adjust size
        }
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

    public void putIfAbsent(LogOperation key) {
        // insertKey() inserts the key if a slot if found and returns the index
        int index = insertKey(key);
        if (index < 0) {
            return;
        }
        doPut(key, index);
    }

    public void put(LogOperation key) {
        // insertKey() inserts the key if a slot if found and returns the index
        int index = insertKey(key);
        doPut(key, index);
    }
}
