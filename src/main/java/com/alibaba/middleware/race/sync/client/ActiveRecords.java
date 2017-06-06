package com.alibaba.middleware.race.sync.client;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yche on 6/6/17.
 * shared by all client threads
 */
public class ActiveRecords {
    private ConcurrentHashMap<Long, HashMap<String, Object>> activeRecordMap;


}
