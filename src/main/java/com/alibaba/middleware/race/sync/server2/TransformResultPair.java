package com.alibaba.middleware.race.sync.server2;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yche on 6/18/17.
 */
class TransformResultPair {
    final ByteBuffer retByteBuffer; // fast-consumption object
    final ArrayList<RecordKeyValuePair> recordWrapperArrayList; // fast-consumption object

    TransformResultPair(ByteBuffer retByteBuffer, ArrayList<RecordKeyValuePair> recordWrapperArrayList) {
        this.retByteBuffer = retByteBuffer;
        this.recordWrapperArrayList = recordWrapperArrayList;
    }
}
