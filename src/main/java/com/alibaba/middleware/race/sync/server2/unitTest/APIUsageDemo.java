package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

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

        System.out.println("logical cpu num:" + Runtime.getRuntime().availableProcessors());
//        System.out.println("digits:" + RecordScanner.stringBuilder.toString());
//        System.out.println("len:" + RecordScanner.stringBuilder.length());
//        System.out.println("valid num:" + RecordScanner.validNum);
//        System.out.println("invalid num:" + RecordScanner.invalidNum);
    }
}
