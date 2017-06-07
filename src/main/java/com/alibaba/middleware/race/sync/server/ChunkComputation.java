package com.alibaba.middleware.race.sync.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yche on 6/7/17.
 * fast consumption object, each one for an computation
 */
public class ChunkComputation {
    private Map<Long, RecordUpdate> activeKeys = new HashMap<>();
    private Map<Long, RecordUpdate> deadKeys = new HashMap<>();

    public ArrayList<RecordUpdate> compute(ArrayList<String> fileChunk, int upper_idx, int lower_idx) {
        return null;
    }
}
