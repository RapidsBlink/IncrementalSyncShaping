package com.alibaba.middleware.race.sync.client;

import com.alibaba.middleware.race.sync.server.RecordUpdate;
import io.netty.util.internal.ConcurrentSet;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by yche on 6/8/17.
 * static variable: shared by all threads
 * other variable: thread local
 */
public class GlobalComputation {
    final static ConcurrentMap<Long, RecordUpdate> inRangeActiveKeys = new ConcurrentHashMap<>();
    final static ConcurrentSet<Long> outOfRangeActiveKeys = new ConcurrentSet<>();
    final static ConcurrentSet<Long> deadKeys = new ConcurrentSet<>();

    static ArrayList<String> filedList = new ArrayList<>();
    final static ConcurrentMap<Long, String> inRangeRecord = new ConcurrentSkipListMap<>();

    public static boolean isInit = false;
    public static long pkLowerBound;
    public static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
        isInit = true;
    }


    public static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }
}

