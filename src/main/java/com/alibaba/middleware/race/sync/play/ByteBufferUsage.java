package com.alibaba.middleware.race.sync.play;

import java.nio.ByteBuffer;

/**
 * Created by yche on 6/16/17.
 */
public class ByteBufferUsage {
    private static void compareByteBuffer() {
        byte[] srcBytes = new byte[10];
        for (byte i = 0; i < srcBytes.length; i++) {
            srcBytes[i] = i;
        }

        byte[] dstBytes = new byte[srcBytes.length];
        System.arraycopy(srcBytes, 0, dstBytes, 0, srcBytes.length);

        ByteBuffer byteBuffer0 = ByteBuffer.wrap(srcBytes);
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(dstBytes);
        System.out.println(byteBuffer0.position() + ", " + byteBuffer0.limit());
        System.out.println(byteBuffer1.position() + ", " + byteBuffer1.limit());

        System.out.println(byteBuffer0.equals(byteBuffer1));

        System.out.println(byteBuffer0.position() + ", " + byteBuffer0.limit());
        System.out.println(byteBuffer1.position() + ", " + byteBuffer1.limit());
    }

    public static void main(String[] args) {
        compareByteBuffer();
    }
}
