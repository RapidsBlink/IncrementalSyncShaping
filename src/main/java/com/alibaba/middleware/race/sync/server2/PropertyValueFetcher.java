package com.alibaba.middleware.race.sync.server2;

import com.alibaba.middleware.race.sync.Server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by yche on 6/18/17.
 * only one globally since only one middle file
 */
public class PropertyValueFetcher {
    private RandomAccessFile raf;

    public PropertyValueFetcher(String filePath) {
        try {
            raf = new RandomAccessFile(filePath, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            if (Server.logger != null) {
                Server.logger.info("open middle file err");
                Server.logger.info(e.getMessage());
            }
        }
    }

    public String fetchProperty(long globalOffset, short length) {
        byte[] result = new byte[length];
        try {
            raf.seek(globalOffset);
            raf.read(result);
        } catch (IOException e) {
            e.printStackTrace();
            if (Server.logger != null) {
                Server.logger.info("fetch property err");
                Server.logger.info(e.getMessage());
            }
        }
        return new String(result);
    }
}
