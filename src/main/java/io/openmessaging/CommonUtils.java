package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author pengqun.pq
 */
class CommonUtils {

    static void writeLong(long value, ByteBuffer byteBuffer, FileChannel fileChannel) {
        if (!byteBuffer.hasRemaining()) {
            flushBuffer(byteBuffer, fileChannel);
        }
        byteBuffer.putLong(value);
    }

    static void writeBytes(byte[] bytes, ByteBuffer byteBuffer, FileChannel fileChannel) {
        if (!byteBuffer.hasRemaining()) {
            flushBuffer(byteBuffer, fileChannel);
        }
        byteBuffer.put(bytes);
    }

    static void flushBuffer(ByteBuffer byteBuffer, FileChannel fileChannel) {
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException("write error");
        }
        byteBuffer.clear();
    }

    static int fillReadBuffer(ByteBuffer readBuffer, FileChannel fileChannel,
                               long offset, long endOffset, int elemSize) {
        try {
            readBuffer.clear();
            if ((endOffset - offset) * elemSize < readBuffer.capacity()) {
                readBuffer.limit((int) (endOffset - offset) * elemSize);
            }
            int readBytes = fileChannel.read(readBuffer, offset * elemSize);
            readBuffer.flip();
            return readBytes;
        } catch (IOException e) {
            throw new RuntimeException("read error");
        }
    }

    static ThreadLocal<ByteBuffer> createBuffer(int size) {
        return ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(size));
    }
}
