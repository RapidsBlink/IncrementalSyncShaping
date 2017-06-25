package com.alibaba.middleware.race.sync.server2.operations;

import com.alibaba.middleware.race.sync.Server;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation implements Comparable<InsertOperation> {
    static int[] INTEGER_CHINESE_CHAR = {14989440, 14989441, 14989443, 14989449, 14989450, 14989465, 14989712, 14989721, 14989725, 14989964, 14989972, 14989996, 14990010, 14990230, 14991005, 14991023, 14991242, 15041963, 15041965, 15041970, 15042203, 15042712, 15042714, 15043227, 15043249, 15043969, 15044497, 15044749, 15044763, 15044788, 15045032, 15047579, 15048590, 15049897, 15050163, 15050917, 15052185, 15056301, 15056528, 15106476, 15108240, 15108241, 15111567, 15112334, 15112623, 15112630, 15113614, 15113640, 15113879, 15114163, 15118481, 15118751, 15175307, 15176860, 15176880, 15176882, 15176887, 15178634, 15182731, 15240841, 15249306, 15250869, 15303353, 15303569, 15307441, 15307693, 15308725, 15308974, 15309192, 15309736, 15310233, 15313551, 15313816, 15317902};
    private static HashMap<Integer, Byte> indexMap = new HashMap<>();

    static {
        for (int i = 0; i < INTEGER_CHINESE_CHAR.length; i++) {
            indexMap.put(INTEGER_CHINESE_CHAR[i], (byte) i);
        }
    }

    public long relevantKey;
    byte firstNameIndex = -1;
    byte lastNameFirstIndex = -1;
    byte lastNameSecondIndex = -1;
    byte sexIndex = -1;
    short score = -1;
    int score2 = -1;

    public InsertOperation(long relevantKey) {
        this.relevantKey = relevantKey;
    }

    public void changePK(long pk) {
        relevantKey = pk;
    }

    private static String toChineseChar(byte index) {
        int intC = INTEGER_CHINESE_CHAR[index];
        byte[] tmpBytes = new byte[3];
        tmpBytes[0] = (byte) (intC >>> 16);
        tmpBytes[1] = (byte) (intC >>> 8);
        tmpBytes[2] = (byte) (intC >>> 0);
        return new String(tmpBytes);
    }

    String getOneLine() {
        StringBuilder stringBuilder = new StringBuilder();
        // key
        stringBuilder.append(relevantKey).append('\t');
        // first name
        stringBuilder.append(toChineseChar(firstNameIndex)).append('\t');
        // last name
        stringBuilder.append(toChineseChar(lastNameFirstIndex));
        if (lastNameSecondIndex != -1)
            stringBuilder.append(toChineseChar(lastNameSecondIndex));
        stringBuilder.append('\t');
        // sex
        stringBuilder.append(toChineseChar(sexIndex)).append('\t');
        // score
        stringBuilder.append(score).append('\t');
        // score2
        if (score2 != -1)
            stringBuilder.append(score2).append('\t');

        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    public byte[] getOneLineBytes() {
        StringBuilder stringBuilder = new StringBuilder();
        // key
        stringBuilder.append(relevantKey).append('\t');
        // first name
        stringBuilder.append(toChineseChar(firstNameIndex)).append('\t');
        // last name
        stringBuilder.append(toChineseChar(lastNameFirstIndex));
        if (lastNameSecondIndex != -1)
            stringBuilder.append(toChineseChar(lastNameSecondIndex));
        stringBuilder.append('\t');
        // sex
        stringBuilder.append(toChineseChar(sexIndex)).append('\t');
        // score
        stringBuilder.append(score).append('\t');
        // score2
        if (score2 != -1)
            stringBuilder.append(score2).append('\t');

        stringBuilder.setLength(stringBuilder.length() - 1);
        stringBuilder.append('\n');
        return stringBuilder.toString().getBytes();
    }

    private static int toInt(byte[] data, int offset) {
        return (data[offset] & 0xFF) << 16 | (data[1 + offset] & 0xFF) << 8 | (data[2 + offset] & 0xFF);
    }

    public static byte getIndexOfChineseChar(byte[] data, int offset) {
        int intC = toInt(data, offset);
        return indexMap.get(intC);
    }

    public static void addDataIntoByteBuffer(ByteBuffer yourByteBuffer, byte index, ByteBuffer tmpByteBuffer) {
        switch (index) {
            case 0:
                yourByteBuffer.put(getIndexOfChineseChar(tmpByteBuffer.array(), 0));
                break;
            case 1:
                yourByteBuffer.put(getIndexOfChineseChar(tmpByteBuffer.array(), 0));
                if (tmpByteBuffer.limit() == 6)
                    yourByteBuffer.put(getIndexOfChineseChar(tmpByteBuffer.array(), 3));
                else {
                    byte nullByte = -1;
                    yourByteBuffer.put(nullByte);
                }
                break;
            case 2:
                yourByteBuffer.put(getIndexOfChineseChar(tmpByteBuffer.array(), 0));
                break;
            case 3:
                short result = 0;
                for (int i = 0; i < tmpByteBuffer.limit(); i++)
                    result = (short) ((10 * result) + (tmpByteBuffer.get(i) - '0'));
                yourByteBuffer.putShort(result);
                break;
            case 4:
                int resultInt = 0;
                for (int i = 0; i < tmpByteBuffer.limit(); i++)
                    resultInt = ((10 * resultInt) + (tmpByteBuffer.get(i) - '0'));
                yourByteBuffer.putInt(resultInt);
                break;
            default:
                if (Server.logger != null)
                    Server.logger.info("add data error");
                System.err.println("add data error");
        }
    }

    public void updateFirstName(byte firstNameIndex) {
        this.firstNameIndex = firstNameIndex;
    }

    public void updateLastName(byte lastNameFirstIndex, byte lastNameSecondIndex) {
        this.lastNameFirstIndex = lastNameFirstIndex;
        this.lastNameSecondIndex = lastNameSecondIndex;
    }

    public void updateSex(byte sexIndex) {
        this.sexIndex = sexIndex;
    }

    public void updateScore(short score) {
        this.score = score;
    }

    public void updateScore2(int score2) {
        this.score2 = score2;
    }

    @Override
    public int hashCode() {
        return (int) (relevantKey ^ (relevantKey >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof InsertOperation && relevantKey == ((InsertOperation) obj).relevantKey;
    }

    @Override
    public int compareTo(InsertOperation o) {
        return compare(relevantKey, o.relevantKey);
    }

    public static int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }
}
