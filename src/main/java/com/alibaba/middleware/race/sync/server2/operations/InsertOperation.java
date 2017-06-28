package com.alibaba.middleware.race.sync.server2.operations;

import static com.alibaba.middleware.race.sync.server2.RestoreComputation.ycheArr;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation extends NonDeleteOperation {

    public InsertOperation(long relevantKey) {
        super(relevantKey);
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
        System.arraycopy(NonDeleteOperation.BYTES_POINTERS[index], 0, byteArr, offset, 3);
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

    @Override
    public void act() {
        ycheArr[(int) (this.relevantKey)] = this;
    }
}
