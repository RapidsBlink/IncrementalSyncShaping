package com.alibaba.middleware.race.sync.server2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.CHUNK_SIZE;
import static com.alibaba.middleware.race.sync.server2.PipelinedComputation.mediatorTasks;

/**
 * Created by yche on 6/22/17.
 */
class MmapReader {
    private FileChannel fileChannel;

    private int nextIndex;
    private int maxIndex;   // inclusive
    private int lastChunkLength;

    MmapReader(String filePath) throws IOException {
        File file = new File(filePath);
        int fileSize = (int) file.length();
        this.lastChunkLength = fileSize % CHUNK_SIZE != 0 ? fileSize % CHUNK_SIZE : CHUNK_SIZE;

        // 2nd: index info
        this.maxIndex = fileSize % CHUNK_SIZE != 0 ? fileSize / CHUNK_SIZE : fileSize / CHUNK_SIZE - 1;

        // 3rd: fileChannel for reading with mmap
        this.fileChannel = new RandomAccessFile(filePath, "r").getChannel();
    }

    // 1st work
    private void fetchNextMmapChunk() throws IOException {
        int currChunkLength = nextIndex != maxIndex ? CHUNK_SIZE : lastChunkLength;

        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, nextIndex * CHUNK_SIZE, currChunkLength);
        mappedByteBuffer.load();
        if (!RecordField.isInit()) {
            new RecordField(mappedByteBuffer).initFieldIndexMap();
        }

        try {
            mediatorTasks.put(new FileTransformMediatorTask(mappedByteBuffer, currChunkLength));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void fetchChunks() {
        for (nextIndex = 0; nextIndex <= maxIndex; nextIndex++) {
            try {
                fetchNextMmapChunk();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
