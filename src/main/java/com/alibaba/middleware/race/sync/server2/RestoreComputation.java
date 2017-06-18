package com.alibaba.middleware.race.sync.server2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import static com.alibaba.middleware.race.sync.Constants.D_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.I_OPERATION;

/**
 * Created by yche on 6/18/17.
 */
public class RestoreComputation {
    private HashMap<Long, ValueIndexArrWrapper> valueIndexArrMap = new HashMap<>();
    private TreeSet<Long> inRangeKeys = new TreeSet<>();

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

    // used by master thread
    public void parallelEvalAndSend(ExecutorService evalThreadPool) {
        BufferedEvalAndSendTask bufferedTask = new BufferedEvalAndSendTask();
        for (Long inRangeKey : inRangeKeys) {
            if (bufferedTask.isFull()) {
                evalThreadPool.execute(bufferedTask);
                bufferedTask = new BufferedEvalAndSendTask();
            }
            bufferedTask.addData(new RecordObject(inRangeKey, valueIndexArrMap.get(inRangeKey)));
        }
        evalThreadPool.execute(bufferedTask);
    }
}
