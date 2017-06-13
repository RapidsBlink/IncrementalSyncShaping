package com.alibaba.middleware.race.sync.unitTest;

import com.alibaba.middleware.race.sync.server.RecordLazyEval;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.AbstractMap;

import static com.alibaba.middleware.race.sync.Constants.DELETE_OPERATION;

/**
 * Created by yche on 6/7/17.
 */
public class RecordLazyEvalTest {
    private static void prettyPrint(RecordLazyEval recordLazyEval) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(recordLazyEval);
        System.out.println(jsonInString);
    }

    private static void iterateThough(RecordLazyEval recordLazyEval) {
        AbstractMap.SimpleEntry<String, Object> nextEntry;
        while (recordLazyEval.hasNext()) {
            nextEntry = recordLazyEval.next();
            System.out.println(nextEntry.getKey() + "," + nextEntry.getValue());
        }
    }

    private static void testOne(String record, StringBuilder stringBuilder) throws IOException {
        System.out.println(record);
        RecordLazyEval recordLazyEval = new RecordLazyEval(record, stringBuilder);
        prettyPrint(recordLazyEval);
        if (recordLazyEval.operationType != DELETE_OPERATION)
            iterateThough(recordLazyEval);
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        RecordLazyEval.schema = "middleware3";
        RecordLazyEval.table = "student";
        String[] records = {
                "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|",
                "|mysql-bin.00001717148759|1496736165000|middleware3|student|D|id:1:1|1|NULL|",
                "|mysql-bin.00001717148759|1496736165000|middleware3|student|U|id:1:1|1|3|first_name:2:0|徐|依|",
                "|mysql-bin.00001717148759|1496736165000|middleware3|student|U|id:1:1|1|3|"};

        StringBuilder stringBuilder = new StringBuilder();
        for (String record : records) {
            testOne(record, stringBuilder);
        }
    }
}