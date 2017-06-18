package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.unused.Record;

/**
 * Created by yche on 6/18/17.
 */
public class RecordObject {
    long key;
    ValueIndexArrWrapper valueIndexArrWrapper;

    public RecordObject(long key, ValueIndexArrWrapper valueIndexArrWrapper) {
        this.key = key;
        this.valueIndexArrWrapper = valueIndexArrWrapper;
    }

    String getOneLine(PropertyValueFetcher propertyValueFetcher) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key).append('\t');
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            IndexPair indexPair = valueIndexArrWrapper.get(i);
            stringBuilder.append(propertyValueFetcher.fetchProperty(indexPair.offset, indexPair.length)).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
