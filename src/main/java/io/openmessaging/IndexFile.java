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
    private FileChannel aiChannel;
    private List<long[]> metaIndexList;
    private ByteBuffer aiBufferForWrite;

    IndexFile() {
        RandomAccessFile aiFile;
        try {
            aiFile = new RandomAccessFile(DATA_DIR + "ai.data", "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file error");
        }
        this.aiChannel = aiFile.getChannel();
        metaIndexList = new ArrayList<>();
        aiBufferForWrite = ByteBuffer.allocateDirect(WRITE_AI_BUFFER_SIZE);
    }

    void writeA(long a) {
        writeLong(a, aiBufferForWrite, aiChannel);
    }

    void flushABuffer() {
        flushBuffer(aiBufferForWrite, aiChannel);
    }

    int fillReadAIBuffer(ByteBuffer readAIBuffer, long offset, long endOffset) {
        return fillReadBuffer(readAIBuffer, aiChannel, offset, endOffset, KEY_A_BYTE_LENGTH);
    }

    List<long[]> getMetaIndexList() {
        return metaIndexList;
    }
}
