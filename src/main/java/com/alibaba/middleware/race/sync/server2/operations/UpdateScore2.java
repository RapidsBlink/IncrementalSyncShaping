package com.alibaba.middleware.race.sync.server2.operations;

/**
 * Created by yche on 6/24/17.
 */
public class UpdateScore2 extends UpdateOperation {
    int score2 = -1;

    public UpdateScore2(long relevantKey, int score2) {
        super(relevantKey);
        this.score2 = score2;
    }
}
