package com.alibaba.middleware.race.sync.play;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by yche on 6/21/17.
 */
public class HashMapToArray {
    public static void main(String[] args) {
        HashMap<Long, String> hashMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            hashMap.put((long) i, i * i + " hello");
        }

        Long[] strings = hashMap.keySet().toArray(new Long[0]);
        System.out.println(Arrays.toString(strings));
    }
}
