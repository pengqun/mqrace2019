package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.CommonUtils.*;
import static io.openmessaging.Constants.*;

/**
 * @author pengqun.pq
 */
class DataFile {

    private static int dataFileCounter = 0;

    private static ByteBuffer aBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);
    private static ByteBuffer bodyBufferForWrite = ByteBuffer.allocateDirect(WRITE_BODY_BUFFER_SIZE);

    private int startOffset;
    private FileChannel aChannel;
    private FileChannel bodyChannel;

    DataFile(int startOffset) {
        this.startOffset = startOffset;
        int index = dataFileCounter++;
        RandomAccessFile aFile;
        RandomAccessFile bodyFile;
        try {
            aFile = new RandomAccessFile(
                    DATA_DIR + "a" + index + ".data", "rw");
            bodyFile = new RandomAccessFile(
                    DATA_DIR + "body" + index + ".data", "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file error");
        }
        this.aChannel = aFile.getChannel();
        this.bodyChannel = bodyFile.getChannel();
    }

    void writeA(long a) {
        writeLong(a, aBufferForWrite, aChannel);
    }

    void writeBody(byte[] body) {
        writeBytes(body, bodyBufferForWrite, bodyChannel);
    }

    void flushABuffer() {
        flushBuffer(aBufferForWrite, aChannel);
    }

    void flushBodyBuffer() {
        flushBuffer(bodyBufferForWrite, bodyChannel);
    }

    int fillReadABuffer(ByteBuffer readABuffer, long offset, long endOffset) {
        return fillReadBuffer(readABuffer, aChannel, offset - startOffset,
                endOffset - startOffset, KEY_A_BYTE_LENGTH);
    }

    void fillReadBodyBuffer(ByteBuffer readBodyBuffer, long offset, long endOffset) {
        fillReadBuffer(readBodyBuffer, bodyChannel, offset - startOffset,
                endOffset - startOffset, BODY_BYTE_LENGTH);
    }
}
