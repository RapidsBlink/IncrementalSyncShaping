package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.server2.operations.LogOperation;
import gnu.trove.impl.HashFunctions;

/**
 * Created by yche on 6/23/17.
 */
public class YcheHashMap {
    private transient long[] _set;
    private transient LogOperation[] _values;

    YcheHashMap(int initialCapacity) {
        _set = new long[initialCapacity];
        _values = new LogOperation[initialCapacity];
    }

    /**
     * Locates the index of <tt>val</tt>.
     *
     * @param val an <code>long</code> value
     * @return the index of <tt>val</tt> or -1 if it isn't in the set.
     */
    private int index(long val) {
        int hash, index, length;

        length = _set.length;
        hash = HashFunctions.hash(val) & 0x7fffffff;
        index = hash % length;
        long state = _set[index];

        if (state == 0)
            return -1;

        else if (state == val)
            return index;

        return indexRehashed(val, index, hash, state);
    }

    private int indexRehashed(long key, int index, int hash, long state) {
        // see Knuth, p. 529
        int length = _set.length;
        int probe = 1 + (hash % (length - 2));
        final int loopIndex = index;

        do {
            index -= probe;
            if (index < 0) {
                index += length;
            }
            //
            if (state == 0)
                return -1;

            //
            if (key == _set[index])
                return index;
        } while (index != loopIndex);

        return -1;
    }

    /**
     * Locates the index at which <tt>val</tt> can be inserted.  if
     * there is already a value equal()ing <tt>val</tt> in the set,
     * returns that value as a negative integer.
     *
     * @param val an <code>long</code> value
     * @return an <code>int</code> value
     */
    private int insertKey(long val) {
        int hash, index;

        hash = HashFunctions.hash(val) & 0x7fffffff;
        index = hash % _set.length;
        long state = _set[index];


        if (state == 0) {
            insertKeyAt(index, val);

            return index;       // empty, all done
        } else if (_set[index] == val) {
            return -index - 1;   // already stored
        }

        // already FULL or REMOVED, must probe
        return insertKeyRehash(val, index, hash);
    }

    private int insertKeyRehash(long val, int index, int hash) {
        // compute the double hash
        final int length = _set.length;
        int probe = 1 + (hash % (length - 2));
        final int loopIndex = index;

        /**
         * Look until FREE slot or we start to loop
         */
        do {
            index -= probe;
            if (index < 0) {
                index += length;
            }
            long state = _set[index];

            // A FREE slot stops the search
            if (state == 0) {
                insertKeyAt(index, val);
                return index;
            }

            if (_set[index] == val) {
                return -index - 1;
            }

            // Detect loop
        } while (index != loopIndex);

        // We inspected all reachable slots and did not find a FREE one
        // If we found a REMOVED slot we return the first one found

        // Can a resizing strategy be found that resizes the set?
        throw new IllegalStateException("No free or removed slots available. Key set full?!!");
    }

    private void insertKeyAt(int index, long val) {
        _set[index] = val;  // insert value
    }

    public LogOperation get(LogOperation key) {
        int index = index(key.relevantKey);
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
