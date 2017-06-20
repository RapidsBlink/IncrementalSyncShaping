package com.alibaba.middleware.race.sync.server2;

import java.util.HashMap;

import static com.alibaba.middleware.race.sync.Constants.D_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.I_OPERATION;
import static com.alibaba.middleware.race.sync.Constants.U_OPERATION;

/**
 * Created by yche on 6/20/17.
 * here update is same as insert operation
 */
public class TransformComputation {
    HashMap<Long, RecordOperation> recordOperationHashMap = new HashMap<>();

    public RecordOperation insertPk(long pk) {
        RecordOperation recordOperation;
        if ((recordOperation = recordOperationHashMap.get(pk)) == null) {
            recordOperation = new RecordOperation(I_OPERATION);
            recordOperationHashMap.put(pk, recordOperation);
        }

        // change operation type
        recordOperation.operationType = I_OPERATION;
        if (recordOperation.filedValuePointers == null) {
            recordOperation.filedValuePointers = new FieldValueEval[RecordField.FILED_NUM];
        }
        return recordOperation;
    }

    public void deletePk(long pk) {
        RecordOperation recordOperation;
        if ((recordOperation = recordOperationHashMap.get(pk)) == null) {
            recordOperation = new RecordOperation(D_OPERATION);
            recordOperationHashMap.put(pk, recordOperation);
        }

        // change operation type
        recordOperation.operationType = D_OPERATION;
        recordOperation.filedValuePointers = null;
    }

    public RecordOperation updatePk(long pk) {
        RecordOperation recordOperation;
        if ((recordOperation = recordOperationHashMap.get(pk)) == null) {
            recordOperation = new RecordOperation(U_OPERATION);
            recordOperationHashMap.put(pk, recordOperation);

            recordOperation.filedValuePointers = new FieldValueEval[RecordField.FILED_NUM];
            for (byte i = 0; i < RecordField.FILED_NUM; i++) {
                recordOperation.filedValuePointers[i] = new FieldValueLazyEval(pk, i);
            }
        }

        recordOperation.operationType = U_OPERATION;
        return recordOperation;
    }

    public void updateProperty(RecordOperation recordOperation, int propertyIndex, byte[] propertyBytes) {
        recordOperation.filedValuePointers[propertyIndex] = new FieldValueEagerEval(propertyBytes);
    }

    // update key: make sure updatePk and updateProperty Invoke first
    public void updatePkTransferProperties(long prevKey, long curKey) {
        // update-property done by users first, then delete, insert
        RecordOperation prevOperation = recordOperationHashMap.get(prevKey);
        RecordOperation curOperation = insertPk(curKey);
        curOperation.filedValuePointers = prevOperation.filedValuePointers;

        deletePk(prevKey);
    }
}
