package com.alibaba.middleware.race.sync.play;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

/**
 * Created by yche on 6/21/17.
 */
public class ChineseCharToInt {

    static int[] INTEGER_CHINESE_CHAR = {-474513408, -474513152, -474512640, -474511104, -474510848, -474507008, -474443776, -474441472, -474440448, -474379264, -474377216, -474371072, -474367488, -474311168, -474112768, -474108160, -474052096, -461067520, -461067008, -461065728, -461006080, -460875776, -460875264, -460743936, -460738304, -460553984, -460418816, -460354304, -460350720, -460344320, -460281856, -459629824, -459371008, -459036416, -458968320, -458775296, -458450688, -457396992, -457338880, -444552192, -444100608, -444100352, -443248896, -443052544, -442978560, -442976768, -442724864, -442718208, -442657024, -442584320, -441478912, -441409792, -426931456, -426533888, -426528768, -426528256, -426526976, -426079744, -425030912, -410154752, -407987712, -407587584, -394151680, -394096384, -393105152, -393040640, -392776448, -392712704, -392656896, -392517632, -392390400, -391540992, -391473152, -390427136};


    //    DataOutputStream dataOutputStream=new DataOutputStream()
    public static int toInt(byte[] data) {
        System.out.println(Arrays.toString(data));
        int ret = (data[0] << 24) + (data[1] << 16) + (data[2] << 8) + (0 << 0);
        return ret;
    }

    public static String toChineseChar(byte index) {
        int intC = INTEGER_CHINESE_CHAR[index];
        byte[] bytes = new byte[3];
        bytes[0] = (byte) ((intC >>> 24) & 0xFF);
        bytes[1] = (byte) ((intC >>> 16) & 0xFF);
        bytes[2] = (byte) ((intC >>> 8) & 0xFF);
        System.out.println(Arrays.toString(bytes));
        return new String(bytes);
    }


    public static void main(String[] args) {
        String[] chineseC = {"一", "丁", "七", "三", "上", "丙", "乐", "乙", "九", "二", "五", "京", "人", "他", "依", "侯", "俊", "八",
                "六", "兲", "军", "刘", "刚", "力", "励", "十", "发", "名", "君", "吴", "周", "四", "城", "天", "女", "娥", "孙", "彭", "徐",
                "恬", "成", "我", "敏", "明", "景", "晶", "李", "杨", "林", "柳", "民", "江", "王", "甜", "田", "甲", "男", "益", "立", "莉"
                , "诚", "赵", "邹", "郑", "钱", "铭", "闵", "阮", "陈", "雨", "静", "骏", "高", "黎" };

        HashSet<Integer> set = new HashSet();
        int[] intC = new int[74];
        for (int i = 0; i < 74; i++) {
//            System.out.println(chineseC[i].getBytes().length);
            set.add(toInt(chineseC[i].getBytes()));
            intC[i] = toInt(chineseC[i].getBytes());
        }
        Arrays.sort(intC);
        System.out.println(set.size());

        System.out.println(Arrays.toString(intC));

        for (int i = 0; i < 74; i++) {
            System.out.print(toChineseChar((byte) i) + ",");
        }
        System.out.println();
        System.out.println(new String(chineseC[1].getBytes()));

        System.out.println(Arrays.toString("一".getBytes()));
        int  i = toInt("一".getBytes()) ;

        byte[] bytes = new byte[3];
        bytes[0] = (byte)((i >>> 24) & 0xFF);
        bytes[1] = (byte)((i >>> 16) & 0xFF);
        bytes[2] = (byte)((i >>> 8) & 0xFF);
        System.out.println(Arrays.toString(bytes));

    }
}
