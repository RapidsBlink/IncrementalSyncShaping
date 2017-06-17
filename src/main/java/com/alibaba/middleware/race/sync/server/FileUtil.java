package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.server2.FileTransformWriteMediator;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * Created by yche on 6/11/17.
 */
final public class FileUtil {
    public static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void transferFile(String fileName, String srcFolder, String dstFolder) throws IOException {
        FileTransformWriteMediator fileTransformWriteMediator = new FileTransformWriteMediator(fileName, srcFolder, dstFolder);
        fileTransformWriteMediator.transformFile();
    }
}
