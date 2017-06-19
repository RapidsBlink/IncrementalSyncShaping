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
        PipelinedComputation.findResultListener = new PipelinedComputation.FindResultListener() {
            @Override
            public void sendToClient(String result) {
//                System.out.println(result);
            }
        };
        FirstPhaseComputationTest.firstPhaseComp();
        PipelinedComputation.secondPhaseComputation();

//        System.out.println("final map size:"+PipelinedComputation.finalResultMap.size());
        // write output
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/yche_me.txt"));
        for (String line : PipelinedComputation.finalResultMap.values()) {
            bufferedWriter.write(line);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
    }
}
