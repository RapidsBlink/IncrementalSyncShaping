package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.RecordKeyValuePair;
import com.alibaba.middleware.race.sync.server2.RecordScanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yche on 6/18/17.
 */
public class RecordScannerTest {
    public static void main(String[] args) throws IOException {
        // 1st: init record field
        RecordFieldTest.initRecordField();

        // our test lines
        StringBuilder stringBuilder = new StringBuilder("|mysql-bin.000018822778510|1497264580000|middleware5|student|I|id:1:1|NULL|1|first_name:2:0|NULL|郑|last_name:2:0|NULL|静依|sex:2:0|NULL|女|score:1:0|NULL|70|").append('\n');
        stringBuilder.append("|mysql-bin.000018822778511|1497264580000|middleware5|student|I|id:1:1|NULL|2|first_name:2:0|NULL|李|last_name:2:0|NULL|莉|sex:2:0|NULL|女|score:1:0|NULL|78|").append('\n');
        stringBuilder.append("|mysql-bin.00001993819932|1497265289000|middleware5|student|U|id:1:1|4231018|4231018|first_name:2:0|王|郑|").append('\n');
        byte[] myBytes = stringBuilder.toString().getBytes();
        System.out.println("current record string line:" + new String(myBytes));

        // usage of RecordScanner
        ByteBuffer byteBuffer = ByteBuffer.wrap(myBytes);
        ByteBuffer retByteBuffer = ByteBuffer.allocate(1024 * 1024);
        ArrayList<RecordKeyValuePair> recordKeyValuePairArrayList = new ArrayList<>();
        RecordScanner recordScanner = new RecordScanner(byteBuffer, 0, byteBuffer.limit(), retByteBuffer, recordKeyValuePairArrayList);
        recordScanner.compute();

        retByteBuffer.flip();
        System.out.println(new String(retByteBuffer.array(), 0, retByteBuffer.limit()));
    }
}
