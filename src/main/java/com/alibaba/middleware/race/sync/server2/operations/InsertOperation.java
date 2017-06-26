package com.alibaba.middleware.race.sync.server2.operations;

import com.alibaba.middleware.race.sync.Server;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation implements Comparable<InsertOperation> {
    private static int[] INTEGER_CHINESE_CHAR = {14989440, 14989441, 14989443, 14989449, 14989450, 14989465, 14989712, 14989721, 14989725, 14989964, 14989972, 14989996, 14990010, 14990230, 14991005, 14991023, 14991242, 15041963, 15041965, 15041970, 15042203, 15042712, 15042714, 15043227, 15043249, 15043969, 15044497, 15044749, 15044763, 15044788, 15045032, 15047579, 15048590, 15049897, 15050163, 15050917, 15052185, 15056301, 15056528, 15106476, 15108240, 15108241, 15111567, 15112334, 15112623, 15112630, 15113614, 15113640, 15113879, 15114163, 15118481, 15118751, 15175307, 15176860, 15176880, 15176882, 15176887, 15178634, 15182731, 15240841, 15249306, 15250869, 15303353, 15303569, 15307441, 15307693, 15308725, 15308974, 15309192, 15309736, 15310233, 15313551, 15313816, 15317902};
    private static HashMap<Integer, Byte> indexMap = new HashMap<>();
    private static byte[][] BYTES_POINTERS = new byte[INTEGER_CHINESE_CHAR.length][];

    static {
        for (byte i = 0; i < INTEGER_CHINESE_CHAR.length; i++) {
            indexMap.put(INTEGER_CHINESE_CHAR[i], i);
            BYTES_POINTERS[i] = InsertOperation.toChineseChar(i).getBytes();
        }
    }

    public long relevantKey;
    public byte firstNameIndex = -1;
    public byte lastNameFirstIndex = -1;
    public byte lastNameSecondIndex = -1;
    public byte sexIndex = -1;
    public short score = -1;
    public int score2 = -1;

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
        tmpBytes[2] = (byte) (intC);
        return new String(tmpBytes);
    }

    private static int getLongLen(long pk) {
        int noOfDigit = 1;
        while ((pk = pk / 10) != 0)
            ++noOfDigit;
        return noOfDigit;
    }

    private static void parseLong(long pk, byte[] byteArr, int offset, int noDigits) {
        long leftLong = pk;
        for (int i = 0; i < noDigits; i++) {
            byteArr[offset + noDigits - i - 1] = (byte) (leftLong % 10 + '0');
            leftLong /= 10;
        }
    }

    private static void parseSingleChar(byte index, byte[] byteArr, int offset) {
        System.arraycopy(BYTES_POINTERS[index], 0, byteArr, offset, 3);
    }


    public byte[] getOneLineBytesEfficient() {
        byte[] tmpBytes = new byte[48];
        int nextOffset = 0;
        // 1st: pk
        int pkDigits = getLongLen(relevantKey);
        parseLong(relevantKey, tmpBytes, nextOffset, pkDigits);
        nextOffset += pkDigits;
        tmpBytes[nextOffset] = '\t';
        nextOffset += 1;

        // 2nd: first name
        parseSingleChar(firstNameIndex, tmpBytes, nextOffset);
        nextOffset += 3;
        tmpBytes[nextOffset] = '\t';
        nextOffset += 1;

        // 3rd: second name
        parseSingleChar(lastNameFirstIndex, tmpBytes, nextOffset);
        nextOffset += 3;
        if (lastNameSecondIndex != -1) {
            parseSingleChar(lastNameSecondIndex, tmpBytes, nextOffset);
            nextOffset += 3;
        }
        tmpBytes[nextOffset] = '\t';
        nextOffset += 1;

        // 4th: sex
        parseSingleChar(sexIndex, tmpBytes, nextOffset);
        nextOffset += 3;
        tmpBytes[nextOffset] = '\t';
        nextOffset += 1;

        // 5th score
        pkDigits = getLongLen(score);
        parseLong(score, tmpBytes, nextOffset, pkDigits);
        nextOffset += pkDigits;
        tmpBytes[nextOffset] = '\t';
        nextOffset += 1;

        // 6th score2
        if (score2 != -1) {
            pkDigits = getLongLen(score2);
            parseLong(score2, tmpBytes, nextOffset, pkDigits);
            nextOffset += pkDigits;
            tmpBytes[nextOffset] = '\t';
            nextOffset += 1;
        }
        tmpBytes[nextOffset - 1] = '\n';

        byte[] retBytes = new byte[nextOffset];
        System.arraycopy(tmpBytes, 0, retBytes, 0, nextOffset);
        return retBytes;
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

    private static int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }
}
