package com.alibaba.middleware.race.sync.play;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by yche on 6/6/17.
 */
public class FileStatistics {
    private static void readFile(String fileName, String schema, String table) throws IOException {
        long startTime = System.currentTimeMillis();

        File logFile = new File("/tmp/canal.txt");
        BufferedReader fileReader = new BufferedReader(new FileReader(fileName));
        ArrayList<String> strList = new ArrayList<>();
        String line;
        int count = 0;
        while ((line = fileReader.readLine()) != null) {
            count++;
            // 1st step: schema, table filter to reduce memory usage
            if (RecordUtil.isSchemaTableOkay(line, schema, table)) {
                strList.add(line);
            }
        }

        long endTime = System.currentTimeMillis();

        System.out.println("all line num:" + count);
        System.out.println("filtered line num:" + strList.size());
        System.out.println("file bytes:" + logFile.length());
        System.out.println("average byte per log:" + (float) logFile.length() / count);
        System.out.println("read time:" + (endTime - startTime) + " ms");
    }

    public static void main(String[] args) throws IOException {
        String filteredSchema = "middleware3";
        String filteredTable = "student";
        readFile("/tmp/canal.txt", filteredSchema, filteredTable);
    }
}
