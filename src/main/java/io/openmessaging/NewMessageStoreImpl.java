package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.openmessaging.Constants.*;

/**
 * @author .ignore 2019-07-29
 */
public class NewMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(NewMessageStoreImpl.class);

    // Assumptions
    private static final long T_UPPER_LIMIT = Long.MAX_VALUE;
    private static final long A_UPPER_LIMIT = Long.MAX_VALUE;
    private static final int MSG_COUNT_UPPER_LIMIT = Integer.MAX_VALUE;

    private static final int DATA_SEGMENT_SIZE = 4 * 1024 * 1024;
//    private static final int DATA_SEGMENT_SIZE = 99 * 1000;

    // TODO split into multiple indexes
    private static final int T_INDEX_SIZE = 1200 * 1024 * 1024;
    private static final int T_INDEX_SUMMARY_FACTOR = 32;

    private static final int WRITE_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024 * 8;
    private static final int WRITE_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;
    private static final int READ_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 256;

    private List<DataFile> dataFileList = new ArrayList<>();
    private DataFile curDataFile = null;

    private ByteBuffer aBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);
    private ByteBuffer bodyBufferForWrite = ByteBuffer.allocateDirect(WRITE_BODY_BUFFER_SIZE);

    private AtomicInteger threadIdCounter = new AtomicInteger(0);
    private ThreadLocal<Integer> threadId = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);

    private int dataFileCounter = 0;
    private volatile boolean rewriteDone = false;
    private short[] tIndex = new short[T_INDEX_SIZE];
    private int[] tIndexSummary = new int[T_INDEX_SIZE / T_INDEX_SUMMARY_FACTOR];

    private ThreadLocal<ByteBuffer> threadBufferForReadA = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BODY_BUFFER_SIZE));

    private StageFile[] stageFileList = new StageFile[PRODUCER_THREAD_NUM];

    private ThreadLocal<StageFile> threadStageFile = ThreadLocal.withInitial(() -> {
        StageFile stageFile = new StageFile();
        int id = threadId.get();
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(Constants.DATA_DIR + "s" + id + ".data", "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("no file");
        }
        stageFile.fileChannel = raf.getChannel();
        stageFile.byteBuffer = ByteBuffer.allocateDirect(MSG_BYTE_LENGTH * 1024);
        stageFileList[id] = stageFile;
        return stageFile;
    });

    private static volatile long tBase = -1;
    private static volatile long[] threadMinT = new long[PRODUCER_THREAD_NUM];

    static {
        Arrays.fill(threadMinT, -1);
        logger.info("LsmMessageStoreImpl loaded");
    }

    @Override
    public void put(Message message) {
        long putStart = System.nanoTime();
        int putId = putCounter.getAndIncrement();
        if (putId == 0) {
            _putStart = System.currentTimeMillis();
        }

        if (tBase < 0) {
            threadMinT[threadId.get()] = message.getT();
            logger.info("Set thread minT for " + threadId.get() + ": " + message.getT());
            if (putId == 0) {
                long min = Long.MAX_VALUE;
                for (int i = 0; i < threadMinT.length; i++) {
                    while (threadMinT[i] < 0) {
                        LockSupport.parkNanos(1_000_000);
                    }
                    min = Math.min(min, threadMinT[i]);
                }
                tBase = min;
                logger.info("Determined T base: " + tBase);
            } else {
                while (tBase < 0) {
                    LockSupport.parkNanos(1_000_000);
                }
            }
        }

//        if (putId == 10000 * 10000) {
//            throw new RuntimeException("" + (System.currentTimeMillis() - _putStart) + ", " + tIndexCounter);
//        }

        StageFile stageFile = threadStageFile.get();
        stageFile.writeMessage(message);

        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("Write message to stage file with t: " + message.getT() + ", a: " + message.getA()
                    + ", time: " + (System.nanoTime() - putStart) + ", putId: " + putId
            );
        }
    }

    private void rewriteFiles() {
        logger.info("Start rewrite files");
        int putCounter = 0;
        int currentT = 0;

        for (StageFile stageFile : stageFileList) {
            stageFile.prepareForRead();
        }

        while (true) {
            int doneCount = 0;
            short writeCount = 0;

            for (StageFile stageFile : stageFileList) {
                if (stageFile.doneRead) {
                    doneCount++;
                    continue;
                }
                Message head;
                while ((head = stageFile.peekMessage()) != null && head.getT() == currentT) {
                    // Got ordered message
                    Message message = stageFile.consumePeeked();
                    if (putCounter % DATA_SEGMENT_SIZE == 0) {
                        if (curDataFile != null) {
                            curDataFile.flushABuffer();
                            curDataFile.flushBodyBuffer();
                        }
                        curDataFile = newDataFile(putCounter, putCounter + DATA_SEGMENT_SIZE - 1);
                    }
                    curDataFile.writeA(message.getA());
                    curDataFile.writeBody(message.getBody());

                    if (putCounter % REWRITE_SAMPLE_RATE == 0) {
                        logger.info("Write message to data file: " + putCounter);
                    }
                    putCounter++;
                    writeCount++;
                }
            }
            if (doneCount == PRODUCER_THREAD_NUM) {
                break;
            }

            // update t index
            //  TODO store overflowed count to additional map
            tIndex[currentT++] = writeCount;

            // update t summary
            if (currentT % T_INDEX_SUMMARY_FACTOR == 0) {
                tIndexSummary[currentT / T_INDEX_SUMMARY_FACTOR] = putCounter;
            }
        }

        curDataFile.flushABuffer();
        curDataFile.flushBodyBuffer();
        logger.info("Done rewrite files, msg count: " + putCounter);
    }

    private long getOffsetByTDiff(int tDiff) {
        long offset = tIndexSummary[tDiff / T_INDEX_SUMMARY_FACTOR];
        for (int i = tDiff / T_INDEX_SUMMARY_FACTOR * T_INDEX_SUMMARY_FACTOR; i < tDiff; i++) {
            offset += tIndex[i];
        }
        return offset;
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        long getStart = System.currentTimeMillis();
        int getId = getCounter.getAndIncrement();
        if (IS_TEST_RUN && getId == 0) {
            _putEnd = System.currentTimeMillis();
            _getStart = _putEnd;
        }
        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("getMessage - tMin: " + tMin + ", tMax: " + tMax
                    + ", aMin: " + aMin + ", aMax: " + aMax + ", getId: " + getId);
        }
        if (!rewriteDone) {
            synchronized (this) {
                if (!rewriteDone) {
                    long totalSize = 0;
                    for (StageFile stageFile : stageFileList) {
                        stageFile.flushBuffer();
                    }
                    logger.info("Flushed all stage files, total size: " + totalSize);

                    rewriteFiles();
                    rewriteDone = true;
                }
                logger.info("Rewrite task has finished, time: " + (System.currentTimeMillis() - getStart));
            }
        }

        ArrayList<Message> result = new ArrayList<>(4096);
        int tDiff = (int) (tMin - tBase);
        long offset = getOffsetByTDiff(tDiff);
        int msgCount;
        long a;
        byte[] body;
        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
        aByteBufferForRead.flip();
        bodyByteBufferForRead.flip();

        for (long t = tMin; t <= tMax; t++) {
            msgCount = tIndex[tDiff++];
            while (msgCount > 0) {
                if (!aByteBufferForRead.hasRemaining()) {
                    fillReadABuffer(aByteBufferForRead, offset);
                }
                a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    if (!bodyByteBufferForRead.hasRemaining()) {
                        fillReadBodyBuffer(bodyByteBufferForRead, offset);
                    }
                    body = new byte[BODY_BYTE_LENGTH];
                    bodyByteBufferForRead.get(body);
                    Message msg = new Message(a, t, body);
                    result.add(msg);
                } else {
                    bodyByteBufferForRead.position(bodyByteBufferForRead.position()
                            + Math.min(bodyByteBufferForRead.remaining(), BODY_BYTE_LENGTH));
                }
                offset++;
                msgCount--;
            }
        }

        aByteBufferForRead.clear();
        bodyByteBufferForRead.clear();

        if (IS_TEST_RUN) {
            getMsgCounter.addAndGet(result.size());
        }
        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("Return sorted result with size: " + result.size()
                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
        }
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long avgStart = System.currentTimeMillis();
        int avgId = avgCounter.getAndIncrement();
        if (IS_TEST_RUN && avgId == 0) {
            _getEnd = System.currentTimeMillis();
            _avgStart = _getEnd;
        }
        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("getAvgValue - tMin: " + tMin + ", tMax: " + tMax
                    + ", aMin: " + aMin + ", aMax: " + aMax + ", avgId: " + avgId);
            if (IS_TEST_RUN && avgId == TEST_BOUNDARY) {
                long putDuration = _putEnd - _putStart;
                long getDuration = _getEnd - _getStart;
                long avgDuration = System.currentTimeMillis() - _avgStart;
                int putScore = (int) (putCounter.get() / putDuration);
                int getScore = (int) (getMsgCounter.get() / getDuration);
                int avgScore = (int) (avgMsgCounter.get() / avgDuration);
                int totalScore = putScore + getScore + avgScore;
                logger.info("Test result: \n"
                        + "\tput: " + putCounter.get() + " / " + putDuration + "ms = " + putScore + "\n"
                        + "\tget: " + getMsgCounter.get() + " / " + getDuration + "ms = " + getScore + "\n"
                        + "\tavg: " + avgMsgCounter.get() + " / " + avgDuration + "ms = " + avgScore + "\n"
                        + "\ttotal: " + totalScore + "\n"
                );
                throw new RuntimeException(putScore + "/" + getScore + "/" + avgScore);
            }
        }

        long sum = 0;
        int count = 0;
        int tDiff = (int) (tMin - tBase);
        long offset = getOffsetByTDiff(tDiff);
        int msgCount;
        long a;
        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        aByteBufferForRead.flip();

        for (long t = tMin; t <= tMax; t++) {
            msgCount = tIndex[tDiff++];
            while (msgCount > 0) {
                if (aByteBufferForRead.remaining() == 0) {
                    fillReadABuffer(aByteBufferForRead, offset);
                }
                a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }
                offset++;
                msgCount--;
            }
        }
        aByteBufferForRead.clear();

        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("Got " + count
                    + ", time: " + (System.currentTimeMillis() - avgStart));
        }
        if (IS_TEST_RUN) {
            avgMsgCounter.addAndGet(count);
        }
        return count > 0 ? sum / count : 0;
    }

    private static class StageFile {
        FileChannel fileChannel;
        ByteBuffer byteBuffer;
        long fileOffset;
        Message peeked;
        boolean doneRead;

        void writeMessage(Message message) {
            if (!byteBuffer.hasRemaining()) {
                flushBuffer();
            }
            byteBuffer.putInt((int) (message.getT() - tBase));
            byteBuffer.putLong(message.getA());
            byteBuffer.put(message.getBody());
        }

        void flushBuffer() {
            byteBuffer.flip();
            try {
                fileChannel.write(byteBuffer);
            } catch (IOException e) {
                throw new RuntimeException("write error");
            }
            byteBuffer.clear();
        }

        void prepareForRead() {
            byteBuffer.flip();
        }

        Message peekMessage() {
            if (peeked != null) {
                return peeked;
            }
            if (!byteBuffer.hasRemaining()) {
                if (!fillReadBuffer()) {
                    return null;
                }
            }
            int t = byteBuffer.getInt();
            long a = byteBuffer.getLong();
            byte[] body = new byte[BODY_BYTE_LENGTH];
            byteBuffer.get(body);

            peeked = new Message(a, t + tBase, body);
            return peeked;
        }

        Message consumePeeked() {
            Message consumed = peeked;
            peeked = null;
            return consumed;
        }

        boolean fillReadBuffer() {
            byteBuffer.clear();
            int readBytes;
            try {
                readBytes = fileChannel.read(byteBuffer, fileOffset);
            } catch (IOException e) {
                throw new RuntimeException("read error");
            }
            if (readBytes <= 0) {
                doneRead = true;
                return false;
            }
            fileOffset += readBytes;
            byteBuffer.flip();
            return true;
        }
    }

    private void fillReadABuffer(ByteBuffer readABuffer, long offset) {
        DataFile dataFile = dataFileList.get((int) (offset / DATA_SEGMENT_SIZE));
        try {
            readABuffer.clear();
            dataFile.aChannel.read(readABuffer, (offset - dataFile.start) * KEY_A_BYTE_LENGTH);
            readABuffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillReadBodyBuffer(ByteBuffer readBodyBuffer, long offset) {
        DataFile dataFile = dataFileList.get((int) (offset / DATA_SEGMENT_SIZE));
        try {
            readBodyBuffer.clear();
            dataFile.bodyChannel.read(readBodyBuffer, (offset - dataFile.start) * BODY_BYTE_LENGTH);
            readBodyBuffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DataFile newDataFile(int start, int end) {
        DataFile dataFile = new DataFile();
        dataFile.index = dataFileCounter++;
        try {
            dataFile.aFile = new RandomAccessFile(
                    Constants.DATA_DIR + "a" + dataFile.index + ".data", "rw");
            dataFile.bodyFile = new RandomAccessFile(
                    Constants.DATA_DIR + "b" + dataFile.index + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        dataFile.aChannel = dataFile.aFile.getChannel();
        dataFile.bodyChannel = dataFile.bodyFile.getChannel();
        dataFile.start = start;
        dataFile.end = end;
        if (dataFile.end < 0) {
            dataFile.end = MSG_COUNT_UPPER_LIMIT;
        }
        dataFileList.add(dataFile);
//        logger.info("Created data file: [" + dataFile.start + ", " + dataFile.end + "]");
        return dataFile;
    }

    private class DataFile {
        int index;
        int start;
        int end;
        RandomAccessFile aFile;
        RandomAccessFile bodyFile;
        FileChannel aChannel;
        FileChannel bodyChannel;

        void writeA(long a) {
            if (!aBufferForWrite.hasRemaining()) {
                flushABuffer();
            }
            aBufferForWrite.putLong(a);
        }

        void writeBody(byte[] body) {
            if (!bodyBufferForWrite.hasRemaining()) {
                flushBodyBuffer();
            }
            bodyBufferForWrite.put(body);
        }

        void flushABuffer() {
            aBufferForWrite.flip();
            try {
                aChannel.write(aBufferForWrite);
            } catch (IOException e) {
                throw new RuntimeException("write error");
            }
            aBufferForWrite.clear();
        }

        void flushBodyBuffer() {
            bodyBufferForWrite.flip();
            try {
                bodyChannel.write(bodyBufferForWrite);
            } catch (IOException e) {
                throw new RuntimeException("write error");
            }
            bodyBufferForWrite.clear();
        }
    }

    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int REWRITE_SAMPLE_RATE = 1000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);

    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;
}
