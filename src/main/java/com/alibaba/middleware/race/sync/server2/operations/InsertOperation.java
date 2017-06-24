package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/19/17.
 */
public class InsertOperation extends NonDeleteOperation {

    public InsertOperation(long relevantKey) {
        super(relevantKey);
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
}
