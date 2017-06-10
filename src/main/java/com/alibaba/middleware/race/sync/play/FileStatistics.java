package com.alibaba.middleware.race.sync.play;

import com.alibaba.middleware.race.sync.server.RecordLazyEval;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static com.alibaba.middleware.race.sync.play.GlobalComputation.OneRound;
import static com.alibaba.middleware.race.sync.play.GlobalComputation.initRange;

/**
 * Created by yche on 6/6/17.
 */
public class FileStatistics {
    public static void main(String[] args) throws IOException {
        RecordLazyEval.schema = "middleware3";
        RecordLazyEval.table = "student";
        initRange(600, 700);
        OneRound("/tmp/canal.txt");
        System.out.println(GlobalComputation.filedList);

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/yche.rs"));
        for (Map.Entry<Long, String> entry : GlobalComputation.inRangeRecord.entrySet()) {
            bufferedWriter.write(entry.getValue());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
    }
}
