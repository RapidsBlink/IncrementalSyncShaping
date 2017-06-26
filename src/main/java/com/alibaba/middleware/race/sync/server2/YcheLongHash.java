package com.alibaba.middleware.race.sync.server2;

import gnu.trove.impl.Constants;
import gnu.trove.impl.HashFunctions;

import java.util.Arrays;

/**
 * Created by yche on 6/24/17.
 */
public class YcheLongHash {
    /**
     * the set of longs
     */
    private transient long[] _set;

    /**
     * value that represents null
     * <p>
     * NOTE: should not be modified after the Hash is created, but is
     * not final because of Externalization
     */
    private long no_entry_value;

    private boolean consumeFreeSlot;

    /**
     * Creates a new <code>YcheLongHash</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     */
    YcheLongHash(int initialCapacity) {
        no_entry_value = Constants.DEFAULT_LONG_NO_ENTRY_VALUE;
        _set = new long[initialCapacity];
        //noinspection RedundantCast
        if (no_entry_value != (long) 0) {
            Arrays.fill(_set, no_entry_value);
        }
    }

    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     *
     * @param initialCapacity an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected void setUp(int initialCapacity) {
        _set = new long[initialCapacity];
    }

    /**
     * Locates the index of <tt>val</tt>.
     *
     * @param val an <code>long</code> value
     * @return the index of <tt>val</tt> or -1 if it isn't in the set.
     */
    int index(long val) {
        int hash, index, length;

        length = _set.length;
        hash = HashFunctions.hash(val) & 0x7fffffff;
        index = hash % length;
        long state = _set[index];

        if (state == no_entry_value)
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
            if (state == no_entry_value)
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
    int insertKey(long val) {
        int hash, index;

        hash = HashFunctions.hash(val) & 0x7fffffff;
        index = hash % _set.length;
        long state = _set[index];

        consumeFreeSlot = false;

        if (state == no_entry_value) {
            consumeFreeSlot = true;
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
        int firstRemoved = -1;

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
            if (state == no_entry_value) {
                consumeFreeSlot = true;
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
}
