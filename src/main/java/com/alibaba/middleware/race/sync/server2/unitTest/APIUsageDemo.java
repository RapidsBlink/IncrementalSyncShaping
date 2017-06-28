package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;
import com.alibaba.middleware.race.sync.server2.RecordScanner;
import com.alibaba.middleware.race.sync.server2.RestoreComputation;
import com.alibaba.middleware.race.sync.server2.operations.DeleteOperation;
import com.alibaba.middleware.race.sync.server2.operations.LogOperation;
import com.alibaba.middleware.race.sync.server2.operations.NonDeleteOperation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by yche on 6/19/17.
 */
public class APIUsageDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
//        Thread.sleep(5000);
        long startTime = System.currentTimeMillis();
        String srcFolder = "/tmp";
        ArrayList<String> filePathList = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            filePathList.add(srcFolder + File.separator + i + ".txt");
        }
        PipelinedComputation.globalComputation(filePathList, 100000, 2000000);

        ByteBuffer byteBuffer = ByteBuffer.allocate(40 * 1024 * 1024);
        PipelinedComputation.putThingsIntoByteBuffer(byteBuffer);
        byteBuffer.flip();
        FileOutputStream bufferedWriter = new FileOutputStream("/tmp/yche_me.txt");
        bufferedWriter.write(byteBuffer.array(), 0, byteBuffer.limit());

        bufferedWriter.close();
        long endTime = System.currentTimeMillis();
        System.out.println("total time:" + (endTime - startTime) + " ms");

//        System.out.println(RestoreComputation.inRangeRecordSet.size());
        System.out.println("logical cpu num:" + Runtime.getRuntime().availableProcessors());
//        System.out.println("digits:" + RecordScanner.stringBuilder.toString());
//        System.out.println("len:" + RecordScanner.stringBuilder.length());
//        System.out.println("valid num:" + RecordScanner.validNum);
//        System.out.println("invalid num:" + RecordScanner.invalidNum);
        short[] globalIndicators = new short[3072];
        Arrays.fill(globalIndicators, (short) 0);
        for (LogOperation logOperation : RestoreComputation.ycheArr) {
            if (logOperation != null) {
                NonDeleteOperation nonDeleteOperation = (NonDeleteOperation) logOperation;
                for (int i = 0; i < nonDeleteOperation.globalIndices.length; i++) {
                    globalIndicators[nonDeleteOperation.globalIndices[i]] = 1;
                }
            }
        }
        for (short index : DeleteOperation.deleteGlobalIndices) {
            if (index != -1)
                globalIndicators[index] = 1;
        }


        int sum = 0;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < globalIndicators.length; i++) {
            if (globalIndicators[i] == 1) {
                stringBuilder.append('1');
                sum++;
            } else {
                stringBuilder.append('0');
            }
        }
        System.out.println(sum);
        System.out.println(stringBuilder.toString());

    }
}
