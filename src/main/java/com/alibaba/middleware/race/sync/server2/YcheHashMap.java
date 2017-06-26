package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.LogOperation;

/**
 * Created by yche on 6/23/17.
 */
public class YcheHashMap {
    private LogOperation[] _values;
    private int slotNum;

    YcheHashMap(int initialCapacity) {
        _values = new LogOperation[initialCapacity];
        slotNum = initialCapacity;
    }

    private int index(LogOperation val) {
        int hash, index;

        hash = val.hashCode() & 0x7fffffff;
        index = hash % slotNum;

        if (_values[index].relevantKey == val.relevantKey)
            return index;

        return indexRehashed(val, index, hash);
    }

    private int indexRehashed(LogOperation key, int index, int hash) {
        // see Knuth, p. 529
        int probe = 1 + (hash % (slotNum - 2));
        final int loopIndex = index;

        do {
            index -= probe;
            if (index < 0) {
                index += slotNum;
            }

            if (key.relevantKey == _values[index].relevantKey)
                return index;
        } while (index != loopIndex);

        return -1;
    }

    public LogOperation get(LogOperation key) {
        int index = index(key);
        return index < 0 ? null : _values[index];
    }

    private void insertKeyAt(int index, LogOperation val) {
        _values[index] = val;  // insert value
    }

    private void insertKey(LogOperation val) {
        int hash, index;

        hash = val.hashCode() & 0x7fffffff;
        index = hash % slotNum;
        LogOperation state = _values[index];

        if (state == null) {
            insertKeyAt(index, val);
            return;
        } else if (state.relevantKey == val.relevantKey) {
            return;
        }

        // already FULL or REMOVED, must probe
        insertKeyRehash(val, index, hash);
    }

    private void insertKeyRehash(LogOperation val, int index, int hash) {
        // compute the double hash
        int probe = 1 + (hash % (slotNum - 2));
        final int loopIndex = index;

        do {
            index -= probe;
            if (index < 0) {
                index += slotNum;
            }
            LogOperation state = _values[index];

            // A FREE slot stops the search
            if (state == null) {
                insertKeyAt(index, val);
                return;
            }

            if (state.relevantKey == val.relevantKey) {
                return;
            }
            // Detect loop
        } while (index != loopIndex);
    }

    public void put(LogOperation key) {
        insertKey(key);
    }
}
