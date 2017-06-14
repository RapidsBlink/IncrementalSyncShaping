package com.alibaba.middleware.race.sync.server;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yche on 6/11/17.
 */
final class FileUtil {
    static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    static void readFileIntoPageCache(String filePath) throws IOException {
        File file = new File(filePath);
        int fileSize = (int) file.length();

        int CHUNK_SIZE = 64 * 1024 * 1024;
        int maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        int lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;
        FileChannel fileChannel = new RandomAccessFile(filePath, "r").getChannel();
        MappedByteBuffer mappedByteBuffer;
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, maxIndex * CHUNK_SIZE, lastChunkLength);
        mappedByteBuffer.load();
        unmap(mappedByteBuffer);
        for (int i = maxIndex - 1; i >= 0; i--) {
            //System.out.println(i);
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i * CHUNK_SIZE, CHUNK_SIZE);
            mappedByteBuffer.load();
            unmap(mappedByteBuffer);
        }
        //System.out.println(maxIndex);

    }
}
