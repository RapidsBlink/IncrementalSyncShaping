package com.alibaba.middleware.race.sync.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yche on 6/8/17.
 */
public class Play {
    public static void main(String[] args) {
        ArrayList<String> myStrList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            myStrList.add("str" + i);
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (String myStr : myStrList) {
            stringBuilder.append(myStr).append('\t');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        System.out.println(stringBuilder.toString());

        List<String> myList = Arrays.asList(stringBuilder.toString().split("\t"));
        System.out.println(myList);
    }
}
