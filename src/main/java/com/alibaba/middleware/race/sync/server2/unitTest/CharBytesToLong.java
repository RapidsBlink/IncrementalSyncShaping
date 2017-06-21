package com.alibaba.middleware.race.sync.server2.unitTest;

import java.util.Arrays;

/**
 * Created by yche on 6/19/17.
 */
public class CharBytesToLong {
    public static long toLong(byte[] bytes, int length) {
        int base = 10;
        long result = 0l;
        for(int i = 0; i <length; i++){
            result = (base * result) + (bytes[i] - '0');
        }
        return result;
    }


    public static void main(String[] args){
        byte[] bytes = (new String("1234")).getBytes();
        System.out.println(Arrays.toString(bytes));
        System.out.println(toLong(bytes, bytes.length));
    }
}
