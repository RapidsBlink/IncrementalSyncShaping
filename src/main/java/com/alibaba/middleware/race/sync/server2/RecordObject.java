package com.alibaba.middleware.race.sync.server2;


/**
 * Created by yche on 6/18/17.
 */
class RecordObject {
    final long key;
    private final ValueArrWrapper valueIndexArrWrapper;

    RecordObject(long key, ValueArrWrapper valueIndexArrWrapper) {
        this.key = key;
        this.valueIndexArrWrapper = valueIndexArrWrapper;
    }

    String getOneLine() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key).append('\t');
        for (int i = 0; i < RecordField.FILED_NUM; i++) {
            byte[] bytes = valueIndexArrWrapper.get(i);
            stringBuilder.append(new String(bytes)).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
