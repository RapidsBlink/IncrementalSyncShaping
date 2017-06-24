package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by yche on 6/19/17.
 */
public class TotalComputationTest {
    public static void main(String[] args) throws IOException, InterruptedException {

        FirstPhaseComputationTest.firstPhaseComp();

        long startTime = System.currentTimeMillis();
        PipelinedComputation.secondPhaseComputation();
        // write output
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/yche_me.txt"));
        for (byte[] line : PipelinedComputation.finalResultMap.values()) {
            bufferedWriter.write(new String(line));
//            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        long endTime = System.currentTimeMillis();
        System.out.println("second phase:" + (endTime - startTime) + " ms");
    }
}
