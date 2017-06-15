package com.alibaba.middleware.race.sync.unused;
import java.util.concurrent.ThreadFactory;

/**
 * Created by will on 15/6/2017.
 */


public class MaxPriorityThreadFactory implements ThreadFactory{
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setPriority(Thread.MAX_PRIORITY );
        return t;
    }
}