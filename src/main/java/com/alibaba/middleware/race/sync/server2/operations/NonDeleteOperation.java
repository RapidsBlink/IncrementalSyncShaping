package com.alibaba.middleware.race.sync.server2.operations;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by yche on 6/22/17.
 */
public abstract class NonDeleteOperation extends LogOperation {
    private static int[] INTEGER_CHINESE_CHAR = {14989440, 14989441, 14989443, 14989449, 14989450, 14989465, 14989712, 14989721, 14989725, 14989964, 14989972, 14989996, 14990010, 14990230, 14991005, 14991023, 14991242, 15041963, 15041965, 15041970, 15042203, 15042712, 15042714, 15043227, 15043249, 15043969, 15044497, 15044749, 15044763, 15044788, 15045032, 15047579, 15048590, 15049897, 15050163, 15050917, 15052185, 15056301, 15056528, 15106476, 15108240, 15108241, 15111567, 15112334, 15112623, 15112630, 15113614, 15113640, 15113879, 15114163, 15118481, 15118751, 15175307, 15176860, 15176880, 15176882, 15176887, 15178634, 15182731, 15240841, 15249306, 15250869, 15303353, 15303569, 15307441, 15307693, 15308725, 15308974, 15309192, 15309736, 15310233, 15313551, 15313816, 15317902};
    private static HashMap<Integer, Byte> indexMap = new HashMap<>();
    static byte[][] BYTES_POINTERS = new byte[INTEGER_CHINESE_CHAR.length][];

    static {
        for (byte i = 0; i < INTEGER_CHINESE_CHAR.length; i++) {
            indexMap.put(INTEGER_CHINESE_CHAR[i], i);
            BYTES_POINTERS[i] = toChineseChar(i).getBytes();
        }
    }

    byte firstNameIndex = -1;
    byte lastNameFirstIndex = -1;
    byte lastNameSecondIndex = -1;
    byte sexIndex = -1;
    short score = -1;
    int score2 = -1;

    private static String toChineseChar(byte index) {
        int intC = INTEGER_CHINESE_CHAR[index];
        byte[] tmpBytes = new byte[3];
        tmpBytes[0] = (byte) (intC >>> 16);
        tmpBytes[1] = (byte) (intC >>> 8);
        tmpBytes[2] = (byte) (intC);
        return new String(tmpBytes);
    }

    public NonDeleteOperation(long relevantKey) {
        super(relevantKey);
    }

    private static int toInt(byte[] data, int offset) {
        return (data[offset] & 0xFF) << 16 | (data[1 + offset] & 0xFF) << 8 | (data[2 + offset] & 0xFF);
    }

    private static byte getIndexOfChineseChar(byte[] data, int offset) {
        int intC = toInt(data, offset);
        return indexMap.get(intC);
    }

    public void addData(int index, ByteBuffer byteBuffer) {
        switch (index) {
            case 0:
                firstNameIndex = getIndexOfChineseChar(byteBuffer.array(), 0);
                break;
            case 1:
                lastNameFirstIndex = getIndexOfChineseChar(byteBuffer.array(), 0);
                if (byteBuffer.limit() == 6)
                    lastNameSecondIndex = getIndexOfChineseChar(byteBuffer.array(), 3);
                break;
            case 2:
                sexIndex = getIndexOfChineseChar(byteBuffer.array(), 0);
                break;
            case 3:
                short result = 0;
                for (int i = 0; i < byteBuffer.limit(); i++)
                    result = (short) ((10 * result) + (byteBuffer.get(i) - '0'));
                score = result;
                break;
            case 4:
                int resultInt = 0;
                for (int i = 0; i < byteBuffer.limit(); i++)
                    resultInt = ((10 * resultInt) + (byteBuffer.get(i) - '0'));
                score2 = resultInt;
                break;
//            default:
//                if (Server.logger != null)
//                    Server.logger.info("add data error");
//                System.err.println("add data error");
        }
    }

    public void mergeAnother(NonDeleteOperation nonDeleteOperation) {
        if (nonDeleteOperation.score != -1) {
            this.score = nonDeleteOperation.score;
            return;
        }
        if (nonDeleteOperation.score2 != -1) {
            this.score2 = nonDeleteOperation.score2;
            return;
        }
        if (nonDeleteOperation.firstNameIndex != -1) {
            this.firstNameIndex = nonDeleteOperation.firstNameIndex;
            return;
        }
        if (nonDeleteOperation.lastNameFirstIndex != -1) {
            this.lastNameFirstIndex = nonDeleteOperation.lastNameFirstIndex;
            this.lastNameSecondIndex = nonDeleteOperation.lastNameSecondIndex;
            return;
        }
        if (nonDeleteOperation.sexIndex != -1) {
            this.sexIndex = nonDeleteOperation.sexIndex;
        }
    }
}
