package com.alibaba.middleware.race.sync.play;

import com.alibaba.middleware.race.sync.server.RecordLazyEval;
import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by yche on 6/6/17.
 */
public class FileStatistics {
    private static SequentialRestore sequentialRestore = new SequentialRestore();

    private static void OneRound(String fileName) throws IOException {
        long startTime = System.currentTimeMillis();

        ReversedLinesFileReader reversedLinesFileReader = new ReversedLinesFileReader(new File(fileName), 1024 * 1024, Charset.defaultCharset());
        String line;
        while ((line = reversedLinesFileReader.readLine()) != null) {
            sequentialRestore.compute(line);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("computation time:" + (endTime - startTime));
    }

    public static void main(String[] args) throws IOException {
        RecordLazyEval.schema = "middleware3";
        RecordLazyEval.table = "student";
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
