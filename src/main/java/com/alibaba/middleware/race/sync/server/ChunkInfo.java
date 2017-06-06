package com.alibaba.middleware.race.sync.server;

import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yche on 6/6/17.
 * each server thread has one
 */
public class ChunkInfo {
    final private int fileId;
    final private int chunkId;

    // null if no matched record for (schema, table)
    private Optional<ArrayList<RecordUpdateInfo>> chunkUpdateList;

    // record active record within the chunk
    private HashMap<String, RecordUpdateInfo> activeRecordMap;

    final private ArrayList<String> logStrList;
    final private int startIndex;
    final private int endIndex;

    private StringBuilder dbNameBuilder = new StringBuilder();
    private StringBuilder tableNameBuilder = new StringBuilder();

    public ChunkInfo(int fileId, int chunkId, int startIndex, int endIndex, final ArrayList<String> logStrList) {
        this.fileId = fileId;
        this.chunkId = chunkId;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.logStrList = logStrList;
    }

}
