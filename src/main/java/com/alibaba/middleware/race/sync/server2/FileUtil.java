package com.alibaba.middleware.race.sync.server2;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * Created by yche on 6/20/17.
 */
final public class FileUtil {
    public static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}