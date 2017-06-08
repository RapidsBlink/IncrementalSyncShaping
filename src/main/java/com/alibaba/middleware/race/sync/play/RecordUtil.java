package com.alibaba.middleware.race.sync.play;

import java.util.HashSet;

import static com.alibaba.middleware.race.sync.Constants.SPLIT_CHAR;

/**
 * Created by yche on 6/7/17.
 */

public class RecordUtil {
    public static HashSet<String> hashSet = new HashSet<>();
    public static boolean isSchemaTableOkay(String recordStr, String schema, String table) {
        int curIndex = 0;
        char ch;
        for (int i = 0; i < 2; i++) {
            if (recordStr.charAt(curIndex) == SPLIT_CHAR)
                curIndex++;
            while (recordStr.charAt(curIndex) != SPLIT_CHAR) {
                curIndex++;
            }
        }

        // schema
        curIndex++;
        StringBuilder stringBuilder = new StringBuilder();
        while ((ch = recordStr.charAt(curIndex)) != SPLIT_CHAR) {
            curIndex++;
            stringBuilder.append(ch);
        }


        String schemaString = stringBuilder.toString();

        // table
        curIndex++;
        stringBuilder.setLength(0);
        while ((ch = recordStr.charAt(curIndex)) != SPLIT_CHAR) {
            curIndex++;
            stringBuilder.append(ch);
        }

        String pair = schemaString + '\t' + stringBuilder.toString();

        if(!hashSet.contains(pair)){
            hashSet.add(pair);
        }

        if (!schema.equals(schemaString)) {
            return false;
        }
        return table.equals(stringBuilder.toString());
    }
}
