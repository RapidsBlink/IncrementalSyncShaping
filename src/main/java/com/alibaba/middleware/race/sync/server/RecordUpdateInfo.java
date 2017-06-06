package com.alibaba.middleware.race.sync.server;


import java.util.Map;

import com.google.common.base.Optional;

/**
 * Created by yche on 6/6/17.
 */
public class RecordUpdateInfo {
    //  null if no key update for this record
    Optional<Long> oldKey;
    Long curKey;

    // null if the last operation for the record is `delete it`
    Optional<Map<String, Object>> filedUpdateMap;
}
