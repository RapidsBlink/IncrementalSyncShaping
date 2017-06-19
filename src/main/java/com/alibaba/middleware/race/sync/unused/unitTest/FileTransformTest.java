package com.alibaba.middleware.race.sync.unused.unitTest;

import com.alibaba.middleware.race.sync.server2.PipelinedComputation;
import com.alibaba.middleware.race.sync.server2.FileTransformWriteMediator;

import java.io.IOException;

/**
 * Created by yche on 6/17/17.
 */
public class FileTransformTest {
    private static void transformOneFile(String filePath) throws IOException {
        FileTransformWriteMediator fileTransformWriteMediator = new FileTransformWriteMediator(filePath);
        fileTransformWriteMediator.transformFile();
    }

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        for (int i = 1; i < 11; i++) {
            transformOneFile(i + ".txt");
        }
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);
    }
}
