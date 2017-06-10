package com.alibaba.middleware.race.sync.client;

/**
 * Created by yche on 6/10/17.
 */
public class ClientComputation {
    public static long extractPK(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\t') {
                break;
            }
            sb.append(str.charAt(i));
        }
        return Long.parseLong(sb.toString());
    }
}
