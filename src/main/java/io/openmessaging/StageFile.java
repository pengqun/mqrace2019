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
    private FileChannel fileChannel;
    private ByteBuffer byteBufferForWrite = ByteBuffer.allocateDirect(WRITE_STAGE_BUFFER_SIZE);
    private ByteBuffer byteBufferForRead = ByteBuffer.allocateDirect(READ_STAGE_BUFFER_SIZE);

    private long lastT = 0;
    private long prevT = 0;
    private int overflowIndex = 0;
    private List<Long> overflowList = new ArrayList<>();

    private long readOffset = 0;
    private Message peeked = null;
    private byte[] bodyContainer = new byte[BODY_BYTE_LENGTH];
    private boolean doneRead = false;

    private int readCount = 0;
    private int consumeCount = 0;

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
        if (tDiff < 255) {
            byteBufferForWrite.put((byte) tDiff);
        } else {
            byteBufferForWrite.put((byte) 255);
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

    int overflowSize() {
        return overflowList.size();
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
        if (tDiff == 255) {
            tDiff = overflowList.get(overflowIndex++);
        }
        long t = prevT + tDiff;
        prevT = t;
        long a = byteBufferForRead.getLong();
        byteBufferForRead.get(bodyContainer);

        readCount++;

        peeked = new Message(a, t, bodyContainer);
        return peeked;
    }

    Message consumePeeked() {
        Message consumed = peeked;
        peeked = null;
        consumeCount++;
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

    public long getReadOffset() {
        return readOffset;
    }

    public int getReadCount() {
        return readCount;
    }

    public int getConsumeCount() {
        return consumeCount;
    }
}
