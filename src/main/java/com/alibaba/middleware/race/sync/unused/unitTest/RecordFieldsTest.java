package com.alibaba.middleware.race.sync.unused.unitTest;

import com.alibaba.middleware.race.sync.unused.server.RecordFields;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yche on 6/10/17.
 */
public class RecordFieldsTest {
    public static void main(String[] args) throws IOException {
        String record = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        RecordFields record1 = new RecordFields(record);
        ObjectMapper mapper = new ObjectMapper();
        String jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record1);
        System.out.println(jsonInString);
        System.out.println(Arrays.toString(record1.colOrder.toArray()));
    }
}
