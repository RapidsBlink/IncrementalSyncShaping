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

        // position 0, limit srcBytes.length
        ByteBuffer byteBuffer0 = ByteBuffer.wrap(srcBytes);
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(dstBytes);
        System.out.println(byteBuffer0.position() + ", " + byteBuffer0.limit());
        System.out.println(byteBuffer1.position() + ", " + byteBuffer1.limit());

        System.out.println(byteBuffer0.equals(byteBuffer1));
        // not changed, after the comparision
        System.out.println(byteBuffer0.position() + ", " + byteBuffer0.limit());
        System.out.println(byteBuffer1.position() + ", " + byteBuffer1.limit());

        ByteBuffer byteBuffer2 = ByteBuffer.allocate(1024 * 1024);
        byteBuffer2.put(srcBytes);
        byteBuffer2.flip();
        System.out.println(byteBuffer0.equals(byteBuffer2));

        byte[] anotherBytes = new byte[20];
        for (int i = 0; i < 10; i++) {
            anotherBytes[i] = (byte) (2 * i);
            anotherBytes[i + 10] = (byte) i;
        }

        // directly set position 10, limit 10+10
        ByteBuffer byteBuffer3 = ByteBuffer.wrap(anotherBytes, 10, 10);
        System.out.println(byteBuffer0.equals(byteBuffer3));
    }

    public static void main(String[] args) {
        compareByteBuffer();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        System.out.println(byteBuffer.position() + ", " + byteBuffer.limit());
    }

}
