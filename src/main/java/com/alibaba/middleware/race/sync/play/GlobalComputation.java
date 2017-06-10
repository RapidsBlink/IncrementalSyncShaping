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
    public final static Map<Long, String> inRangeRecord = new TreeMap<>();

    private static long pkLowerBound;
    private static long pkUpperBound;

    public static void initRange(long lowerBound, long upperBound) {
        pkLowerBound = lowerBound;
        pkUpperBound = upperBound;
    }

    static boolean isKeyInRange(long key) {
        return pkLowerBound < key && key < pkUpperBound;
    }

    public static long extractPK(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\t') {
                break;
            }
            sb.append(str.charAt(i));
        }
        return Long.parseLong(sb.toString());
    }
}

