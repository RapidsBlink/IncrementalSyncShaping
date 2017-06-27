package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;
import com.alibaba.middleware.race.sync.server2.RestoreComputation;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yche on 6/19/17.
 */
public class APIUsageDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        Thread.sleep(5000);
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

        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        System.out.println("from jvm start:" + (endTime - bean.getStartTime() + " ms"));
        System.out.println(RestoreComputation.inRangeRecordSet.size());
        System.out.println("logical cpu num:" + Runtime.getRuntime().availableProcessors());

        for(TLongObjectHashMap _map:RestoreComputation.recordMapArr){
            System.out.println(_map.size());
        }
    }
}
