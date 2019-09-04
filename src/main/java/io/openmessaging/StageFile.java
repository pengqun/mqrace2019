package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.Constants.*;

/**
 * @author pengqun.pq
 */
class StageFile {
    private static final int MAX_UNSIGNED_BYTE = 255;

    private FileChannel fileChannel;
    private ByteBuffer byteBufferForWrite = ByteBuffer.allocateDirect(WRITE_STAGE_BUFFER_SIZE);
    private ByteBuffer byteBufferForRead = ByteBuffer.allocateDirect(READ_STAGE_BUFFER_SIZE);
    private List<Long> overflowList = new ArrayList<>();
    private int overflowIndex = 0;
    private long lastT = 0;
    private long prevT = 0;
    private long readOffset = 0;
    private Message peeked = null;
    private boolean doneRead = false;

    StageFile(int index) {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(DATA_DIR + "stage" + index + ".data", "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("no file");
        }
        this.fileChannel = raf.getChannel();
    }

    void writeMessage(Message message) {
        if (!byteBufferForWrite.hasRemaining()) {
            flushBuffer();
        }
        long tDiff = message.getT() - lastT;
        // NOTE: overflowed (>= 255) diff will be stored in additional list
        if (tDiff < MAX_UNSIGNED_BYTE) {
            byteBufferForWrite.put((byte) tDiff);
        } else {
            byteBufferForWrite.put((byte) MAX_UNSIGNED_BYTE);
            overflowList.add(tDiff);
        }
        lastT = message.getT();

        byteBufferForWrite.putLong(message.getA());
        byteBufferForWrite.put(message.getBody());
    }

    void flushBuffer() {
        byteBufferForWrite.flip();
        try {
            fileChannel.write(byteBufferForWrite);
        } catch (IOException e) {
            throw new RuntimeException("write error");
        }
        byteBufferForWrite.clear();
    }

    long getLastT() {
        return lastT;
    }

    long fileSize() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new RuntimeException("size error");
        }
    }

    void prepareForRead() {
        byteBufferForRead.flip();
    }

    Message peekMessage() {
        if (peeked != null) {
            return peeked;
        }
        if (!byteBufferForRead.hasRemaining()) {
            if (!fillReadBuffer()) {
                return null;
            }
        }
        long tDiff = byteBufferForRead.get() & 0xff;
        if (tDiff == MAX_UNSIGNED_BYTE) {
            tDiff = overflowList.get(overflowIndex++);
        }
        long t = prevT + tDiff;
        prevT = t;
        long a = byteBufferForRead.getLong();

        byte[] body= new byte[BODY_BYTE_LENGTH];
        byteBufferForRead.get(body);

        peeked = new Message(a, t, body);
        return peeked;
    }

    Message consumePeeked() {
        Message consumed = peeked;
        peeked = null;
        return consumed;
    }

    private boolean fillReadBuffer() {
        byteBufferForRead.clear();
        int readBytes;
        try {
            readBytes = fileChannel.read(byteBufferForRead, readOffset);
        } catch (IOException e) {
            throw new RuntimeException("read error");
        }
        if (readBytes <= 0) {
            doneRead = true;
            return false;
        }
        readOffset += readBytes;
        byteBufferForRead.flip();
        return true;
    }

    boolean isDoneRead() {
        return doneRead;
    }
}
