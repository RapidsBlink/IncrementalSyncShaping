package com.alibaba.middleware.race.sync.unitTest;

import com.alibaba.middleware.race.sync.server2.LineDirectReader;

import java.io.IOException;

/**
 * Created by yche on 6/16/17.
 */
public class LineDirectReaderTest {
    public static void main(String[] args) throws IOException {
        LineDirectReader lineDirectReader = new LineDirectReader("/tmp/1.txt");
        byte[] nextBytes;
        int i = 0;
        while ((nextBytes = lineDirectReader.readLineBytes()) != null) {
            if (i < 10)
                System.out.println(new String(nextBytes));
            i++;
        }
        System.out.println("line count:" + i);
    }
}
