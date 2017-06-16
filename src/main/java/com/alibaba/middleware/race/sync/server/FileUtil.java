package com.alibaba.middleware.race.sync.server;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import com.alibaba.middleware.race.sync.server2.LineDirectReader;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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

    public static void transferFile(String fileName, String srcFolder, String dstFolder) throws IOException {
        FileChannel srcFileChannel = new RandomAccessFile(srcFolder + File.separator + fileName, "r").getChannel();
        FileChannel dstFileChannel = new RandomAccessFile(dstFolder + File.separator + fileName, "rw").getChannel();

        dstFileChannel.transferFrom(srcFileChannel, 0, srcFileChannel.size());

        srcFileChannel.close();
        dstFileChannel.close();

    }

    static int CHUNK_SIZE = 64 * 1024 * 1024;
    private static byte[] internalBuff = new byte[CHUNK_SIZE];
    //private static byte[] reducedBuff = new byte[CHUNK_SIZE];
    public static void copyFiles(String fileName, String srcFolder, String dstFolder) throws IOException {
        FileChannel srcFileChannel = new RandomAccessFile(srcFolder + File.separator + fileName, "r").getChannel();
        File file = new File(srcFolder + File.separator + fileName);
        long fileSize = file.length();

        MappedFileWriter mappedFileWriter = new MappedFileWriter(dstFolder + File.separator+ fileName, fileSize);
        MappedByteBuffer srcMappedByteBuffer = null;

        long maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;
        long lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;

        for (long nextIndex = 0; nextIndex < maxIndex; nextIndex++) {
            srcMappedByteBuffer = srcFileChannel.map(FileChannel.MapMode.READ_ONLY, nextIndex * CHUNK_SIZE, CHUNK_SIZE);
            srcMappedByteBuffer.load();
            srcMappedByteBuffer.get(internalBuff);

            mappedFileWriter.write(internalBuff);
            unmap(srcMappedByteBuffer);

        }
        srcMappedByteBuffer = srcFileChannel.map(FileChannel.MapMode.READ_ONLY, maxIndex * CHUNK_SIZE, lastChunkLength);
        srcMappedByteBuffer.load();
        srcMappedByteBuffer.get(internalBuff, 0 , srcMappedByteBuffer.limit());
        mappedFileWriter.write(internalBuff, 0, srcMappedByteBuffer.limit());
        unmap(srcMappedByteBuffer);
        mappedFileWriter.close();
    }
}
