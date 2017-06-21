package com.alibaba.middleware.race.sync.play;

import sun.jvm.hotspot.oops.Array;

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
//not thread safe
public class ChineseCharToInt {
    static byte[] tmpBytes = new byte[3];

    private static int[] INTEGER_CHINESE_CHAR = {14989440, 14989441, 14989443, 14989449, 14989450, 14989465, 14989712, 14989721, 14989725, 14989964, 14989972, 14989996, 14990010, 14990230, 14991005, 14991023, 14991242, 15041963, 15041965, 15041970, 15042203, 15042712, 15042714, 15043227, 15043249, 15043969, 15044497, 15044749, 15044763, 15044788, 15045032, 15047579, 15048590, 15049897, 15050163, 15050917, 15052185, 15056301, 15056528, 15106476, 15108240, 15108241, 15111567, 15112334, 15112623, 15112630, 15113614, 15113640, 15113879, 15114163, 15118481, 15118751, 15175307, 15176860, 15176880, 15176882, 15176887, 15178634, 15182731, 15240841, 15249306, 15250869, 15303353, 15303569, 15307441, 15307693, 15308725, 15308974, 15309192, 15309736, 15310233, 15313551, 15313816, 15317902};
    private static int INTEGER_GENDER_MALE = 15176887; // 男

    //男 -> 1, 女 -> 0;
    public static byte getGenderIndex(byte[] data){
        int intC = toInt(data);
        if(intC == INTEGER_GENDER_MALE)
            return 1;
        return 0;
    }
    public static String getGenderString(byte index){
        return index == 1 ? "男":"女";
    }

    public static int toInt(byte[] data) {
        return toInt(data, 0);
    }

    public static int toInt(byte[] data , int offset) {
        int ret = (data[offset] & 0xFF) << 16 | (data[1 + offset] & 0xFF) << 8 | (data[2 + offset] & 0xFF);
        return ret;
    }

    public static byte getIndexOfChineseChar(byte[] data, int offset){
        int intC = toInt(data, offset);
        return (byte)Arrays.binarySearch(INTEGER_CHINESE_CHAR, intC);
    }

    public static String toChineseChar(byte index) {
        int intC = INTEGER_CHINESE_CHAR[index];
        tmpBytes[0] = (byte) (intC >>> 16);
        tmpBytes[1] = (byte) (intC >>> 8);
        tmpBytes[2] = (byte) (intC >>> 0);
        return new String(tmpBytes);
    }


    public static void main(String[] args) {
        System.out.println(toInt("女".getBytes()));
        String name = "丙七";
        byte[] nameBytes = name.getBytes();
        byte index1 = getIndexOfChineseChar(nameBytes, 0);
        byte index2 = getIndexOfChineseChar(nameBytes, 3);

        System.out.println("Char 1: " + toChineseChar(index1));
        System.out.println("Char 2: " + toChineseChar(index2));

        String gender = "男";
        byte genderIndex = getGenderIndex(gender.getBytes());
        System.out.println(getGenderString(genderIndex));

    }



    private static void generateData(){
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
        bytes[0] = (byte)(i >>> 16);
        bytes[1] = (byte)(i >>> 8);
        bytes[2] = (byte)(i >>> 0);
        System.out.println(Arrays.toString(bytes));

        System.out.println(toInt("男".getBytes()));
        System.out.println(toInt("女".getBytes()));

    }
}
