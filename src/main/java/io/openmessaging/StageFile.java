package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.Constants.*;

/**
 * @author pengqun.pq
 */
class StageFile {
    private FileChannel fileChannel;
    private ByteBuffer byteBufferForWrite;
    private ByteBuffer byteBufferForRead;
    private long fileOffset;
    private Message peeked;
    private boolean doneRead;

    StageFile(int index) {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(DATA_DIR + "stage" + index + ".data", "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("no file");
        }
        fileChannel = raf.getChannel();
        byteBufferForWrite = ByteBuffer.allocateDirect(WRITE_STAGE_BUFFER_SIZE);
        byteBufferForRead = ByteBuffer.allocateDirect(READ_STAGE_BUFFER_SIZE);
    }

    void writeMessage(Message message) {
        if (!byteBufferForWrite.hasRemaining()) {
            flushBuffer();
        }
        byteBufferForWrite.putInt((int) message.getT());
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(MSG_BYTE_LENGTH);
        try {
            fileChannel.read(byteBuffer, fileChannel.size() - MSG_BYTE_LENGTH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.flip();
        return byteBuffer.getInt();
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
        int t = byteBufferForRead.getInt();
        long a = byteBufferForRead.getLong();
        byte[] body = new byte[BODY_BYTE_LENGTH];
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
            readBytes = fileChannel.read(byteBufferForRead, fileOffset);
        } catch (IOException e) {
            throw new RuntimeException("read error");
        }
        if (readBytes <= 0) {
            doneRead = true;
            return false;
        }
        fileOffset += readBytes;
        byteBufferForRead.flip();
        return true;
    }

    boolean isDoneRead() {
        return doneRead;
    }
}
