package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateScore extends LogOperation {
    short score = -1;

    public UpdateScore(long relevantKey, short score) {
        super(relevantKey);
        this.score = score;
    }
}
