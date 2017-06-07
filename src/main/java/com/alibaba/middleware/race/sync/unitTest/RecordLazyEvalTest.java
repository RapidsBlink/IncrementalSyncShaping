package com.alibaba.middleware.race.sync.unitTest;

import com.alibaba.middleware.race.sync.server.RecordLazyEval;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.AbstractMap;

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

    public static void main(String[] args) throws IOException {
        String record = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        String record2 = "|mysql-bin.00001717148759|1496736165000|middleware3|student|D|id:1:1|1|NULL|";
        String record3 = "|mysql-bin.00001717148759|1496736165000|middleware3|student|U|id:1:1|1|3|first_name:2:0|徐|依|";

        StringBuilder stringBuilder = new StringBuilder();
        RecordLazyEval recordLazyEval = new RecordLazyEval(record, stringBuilder);
        prettyPrint(recordLazyEval);
        iterateThough(recordLazyEval);

        recordLazyEval = new RecordLazyEval(record2, stringBuilder);
        prettyPrint(recordLazyEval);
        iterateThough(recordLazyEval);

        recordLazyEval = new RecordLazyEval(record3, stringBuilder);
        prettyPrint(recordLazyEval);
        iterateThough(recordLazyEval);
    }
}
