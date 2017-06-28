package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;
import com.alibaba.middleware.race.sync.server2.RestoreComputation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by yche on 6/18/17.
 */
public class FirstPhaseComputationTest {
    private static void firstPhaseComp() throws InterruptedException, IOException {
        Thread.sleep(5000);
        long startTime = System.currentTimeMillis();
        PipelinedComputation.initRange(100000, 2000000);
        String srcFolder = "/tmp";
        ArrayList<String> filePathList = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            filePathList.add(srcFolder + File.separator + i + ".txt");
        }
        PipelinedComputation.firstPhaseComputation(filePathList);
        long endTime = System.currentTimeMillis();


        System.out.println("first phase computation cost:" + (endTime - startTime) + " ms");
//        System.out.println(RestoreComputation.inRangeRecordSet.size());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        firstPhaseComp();
    }
}
