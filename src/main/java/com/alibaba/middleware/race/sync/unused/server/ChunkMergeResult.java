package com.alibaba.middleware.race.sync.unused.server;

import com.alibaba.middleware.race.sync.server.RecordUpdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yche on 6/8/17.
 */
public class ChunkMergeResult {
    final Map<Long, RecordUpdate> activeKeys;
    final Map<Long, RecordUpdate> deadKeys;
    final ArrayList<RecordUpdate> insertOnlyUpdates;

    public ChunkMergeResult(Map<Long, RecordUpdate> activeKeys, Map<Long, RecordUpdate> deadKeys, ArrayList<RecordUpdate> insertOnlyUpdates) {
        this.activeKeys = activeKeys;
        this.deadKeys = deadKeys;
        this.insertOnlyUpdates = insertOnlyUpdates;
    }

    public ChunkMergeResult() {
        activeKeys = new HashMap<>();
        deadKeys = new HashMap<>();
        insertOnlyUpdates = new ArrayList<>();
    }

    private static ChunkMergeResult valueOf(String serializationStr) {
        return null;
    }

    @Override
    public String toString() {
        return "";
    }
}
