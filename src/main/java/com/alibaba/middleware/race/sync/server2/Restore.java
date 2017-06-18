package com.alibaba.middleware.race.sync.server2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.alibaba.middleware.race.sync.Constants.D_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.I_OPERATION;

/**
 * Created by yche on 6/18/17.
 */
public class Restore {
    HashMap<Long, ValueIndexArrWrapper> valueIndexArrMap = new HashMap<>();
    TreeSet<Long> inRangeKeys = new TreeSet<>();
    ConcurrentMap<Long, String> finalResultMap = new ConcurrentSkipListMap<>();

    public void compute(RecordWrapper recordWrapper) {
        KeyOperation keyOperation = recordWrapper.keyOperation;
        if (keyOperation.getOperationType() == D_OPERATION) {
            valueIndexArrMap.remove(keyOperation.getPrevKey());
            if (KeyOperation.isKeyInRange(keyOperation.getPrevKey())) {
                inRangeKeys.remove(keyOperation.getPrevKey());
            }
        } else if (keyOperation.getOperationType() == I_OPERATION) {
            valueIndexArrMap.put(keyOperation.getCurKey(), recordWrapper.valueIndexArrWrapper);
            if (KeyOperation.isKeyInRange(keyOperation.getCurKey())) {
                inRangeKeys.add(keyOperation.getCurKey());
            }
        } else {
            // update
            ValueIndexArrWrapper valueIndexArrWrapper = valueIndexArrMap.get(keyOperation.getPrevKey());
            valueIndexArrWrapper.mergeLatterOperation(recordWrapper.valueIndexArrWrapper);

            if (keyOperation.isKeyChanged()) {
                valueIndexArrMap.remove(keyOperation.getPrevKey());
                valueIndexArrMap.put(keyOperation.getCurKey(), valueIndexArrWrapper);
                if (KeyOperation.isKeyInRange(keyOperation.getPrevKey())) {
                    inRangeKeys.remove(keyOperation.getPrevKey());
                }
                if (KeyOperation.isKeyInRange(keyOperation.getCurKey())) {
                    inRangeKeys.add(keyOperation.getCurKey());
                }
            }
        }
    }

    public ArrayList<ValueIndexArrWrapper> getInRangeValueArrWrappers() {
        ArrayList<ValueIndexArrWrapper> valueIndexArrWrapperArrayList = new ArrayList<>(inRangeKeys.size());
        for (Long inRangeKey : inRangeKeys) {
            valueIndexArrWrapperArrayList.add(valueIndexArrMap.get(inRangeKey));
        }
        return valueIndexArrWrapperArrayList;
    }

    public void addFinalResult(long key, String result) {
        finalResultMap.put(key, result);
    }
}
