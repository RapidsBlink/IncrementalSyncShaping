package com.alibaba.middleware.race.sync.unitTest;

import com.alibaba.middleware.race.sync.server2.LineDirectReader;

import java.io.IOException;

/**
 * Created by yche on 6/16/17.
 */
public class LineDirectReaderTest {

    private static void directFileReaderTest(String filePath) throws IOException {
        System.out.println(filePath);
        LineDirectReader lineDirectReader = new LineDirectReader(filePath);
        byte[] nextBytes;
        int i = 0;
        while ((nextBytes = lineDirectReader.readLineBytes()) != null) {
            if (i < 10)
                System.out.println("start" + new String(nextBytes));
            i++;
        }
        System.out.println("line count:" + i);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        Thread.sleep(5000);
        for (int i = 1; i < 11; i++)
            directFileReaderTest("/tmp/" + i + ".txt");
    }
}
