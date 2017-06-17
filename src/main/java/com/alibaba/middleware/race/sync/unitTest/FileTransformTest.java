package com.alibaba.middleware.race.sync.unitTest;

import com.alibaba.middleware.race.sync.server2.FileTransformComputation;
import com.alibaba.middleware.race.sync.server2.FileTransformWriteMediator;

import java.io.IOException;

/**
 * Created by yche on 6/17/17.
 */
public class FileTransformTest {
    private static void transformOneFile(String fileName) throws IOException {
        FileTransformWriteMediator fileTransformWriteMediator = new FileTransformWriteMediator(fileName, "/tmp", "/home/yche/OutData");
        fileTransformWriteMediator.transformFile();
    }

    public static void main(String[] args) throws IOException {
        for (int i = 1; i < 11; i++) {
            transformOneFile(i + ".txt");
        }
        FileTransformComputation.joinPool();
    }
}
