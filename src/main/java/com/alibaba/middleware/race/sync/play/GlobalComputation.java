package com.alibaba.middleware.race.sync.play;

import com.alibaba.middleware.race.sync.server.RecordUpdate;

import java.util.*;

/**
 * Created by yche on 6/8/17.
 */
public class GlobalComputation {
    final static Map<Long, RecordUpdate> inRangeActiveKeys = new HashMap<>();
    final static Set<Long> outOfRangeActiveKeys = new HashSet<>();
    final static Set<Long> deadKeys = new HashSet<>();

    static ArrayList<String> filedList = new ArrayList<>();
    final static Map<Long, String> inRangeRecord = new TreeMap<>();

    public static boolean isInit = false;
    public static long pkLowerBound;
    public static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
        isInit = true;
    }

    static {
        initRange(600, 700);
    }

    public static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }
}

