package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.CommonUtils.*;
import static io.openmessaging.Constants.*;

/**
 * @author pengqun.pq
 */
class IndexFile {
    private static int level = 0;
    private FileChannel aiChannel;
    private List<long[]> metaIndexList = new ArrayList<>();
    private List<Long> rangeSumList = new ArrayList<>();
    private ByteBuffer aiBufferForWrite = ByteBuffer.allocateDirect(WRITE_AI_BUFFER_SIZE);

    IndexFile() {
        RandomAccessFile aiFile;
        try {
            aiFile = new RandomAccessFile(DATA_DIR + "ai" + (level++) + ".data", "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file error");
        }
        this.aiChannel = aiFile.getChannel();
    }

    void writeA(long a) {
        writeLong(a, aiBufferForWrite, aiChannel);
    }

    void flushABuffer() {
        flushBuffer(aiBufferForWrite, aiChannel);
    }

    void fillReadAIBuffer(ByteBuffer readAIBuffer, long offset, long endOffset) {
        fillReadBuffer(readAIBuffer, aiChannel, offset, endOffset, KEY_A_BYTE_LENGTH);
    }

    void addMetaIndex(long[] metaIndex) {
        metaIndexList.add(metaIndex);
    }

    long[] getMetaIndex(int index) {
        return metaIndexList.get(index);
    }

    long getRangeSum(int index) {
        return rangeSumList.get(index);
    }

    void addRangeSum(long rangeSum) {
        rangeSumList.add(rangeSum);
    }
}
