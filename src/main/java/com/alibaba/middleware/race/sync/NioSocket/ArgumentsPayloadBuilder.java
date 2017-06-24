package com.alibaba.middleware.race.sync.NioSocket;

/**
 * Created by will on 8/6/2017.
 */
public class ArgumentsPayloadBuilder {
    public String[] args;

    private char SPLIT = '\t';

    public ArgumentsPayloadBuilder(String[] args) {
        this.args = args;
    }

    public ArgumentsPayloadBuilder(String argsString) {
        args = new String[4];
        int currentIndex = 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < argsString.length(); i++) {
            if (argsString.charAt(i) != SPLIT) {
                sb.append(argsString.charAt(i));
            } else {
                args[currentIndex++] = sb.toString();
                sb.setLength(0);
            }
        }
    }

    @Override
    public String toString() {
        return args[0] + SPLIT + args[1] + SPLIT + args[2] + SPLIT + args[3] + SPLIT;
    }
}
