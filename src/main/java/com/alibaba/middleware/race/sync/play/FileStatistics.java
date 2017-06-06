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
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        File logFile = new File("/tmp/canal.txt");
        BufferedReader fileReader = new BufferedReader(new FileReader("/tmp/canal.txt"));
        ArrayList<String> strList = new ArrayList<>();
        String line;
        int count = 0;
        while ((line = fileReader.readLine()) != null) {
            count++;
            strList.add(line);
        }
        long endTime = System.currentTimeMillis();

        System.out.println("log line num:" + strList.size());
        System.out.println("bytes:" + logFile.length());
        System.out.println("avg byte per log:" + (float) logFile.length() / count);
        System.out.println("read time:" + (endTime - startTime) + " ms");
    }
}
