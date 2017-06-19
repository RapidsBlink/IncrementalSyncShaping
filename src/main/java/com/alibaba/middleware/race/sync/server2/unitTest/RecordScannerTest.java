package com.alibaba.middleware.race.sync.server2.unitTest;

import com.alibaba.middleware.race.sync.server2.LogOperation;
import com.alibaba.middleware.race.sync.server2.RecordScanner;
import org.codehaus.jackson.map.ObjectMapper;

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
        stringBuilder.append("|mysql-bin.000018822778559|1497264580000|middleware5|student|U|id:1:1|20|50|first_name:2:0|yche|yche2|").append('\n');
        stringBuilder.append("|mysql-bin.000018822778559|1497264580000|middleware5|student|D|id:1:1|20|NULL|").append('\n');
        byte[] myBytes = stringBuilder.toString().getBytes();
        System.out.println("current record string line:" + new String(myBytes));

        // usage of RecordScanner
        ByteBuffer byteBuffer = ByteBuffer.wrap(myBytes);
        ArrayList<LogOperation> recordKeyValuePairArrayList = new ArrayList<>();
        RecordScanner recordScanner = new RecordScanner(byteBuffer, 0, byteBuffer.limit(), recordKeyValuePairArrayList);
        recordScanner.compute();

        // check retByteBuffer correctly work
        RecordScanner recordScanner2 = new RecordScanner(byteBuffer, 0, byteBuffer.limit(),recordKeyValuePairArrayList);
        recordScanner2.compute();

        // output result
        for (LogOperation recordKeyValuePair : recordKeyValuePairArrayList) {
            ObjectMapper mapper = new ObjectMapper();
            String jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(recordKeyValuePair);
            System.out.println(jsonInString);
        }
    }
}
