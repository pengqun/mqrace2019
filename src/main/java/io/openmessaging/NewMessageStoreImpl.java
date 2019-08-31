package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.openmessaging.Constants.*;

/**
 * @author .ignore 2019-07-29
 */
@SuppressWarnings("DuplicatedCode")
public class NewMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(NewMessageStoreImpl.class);

    private static final int MSG_COUNT_UPPER_LIMIT = Integer.MAX_VALUE;

    private static final int DATA_SEGMENT_SIZE = 8 * 1024 * 1024;

    private static final int T_INDEX_SUMMARY_FACTOR = 32;

    private static final int A_INDEX_BLOCK_SIZE = 1024 * 4;
    private static final int A_INDEX_META_FACTOR = 16;

    private static final int WRITE_STAGE_BUFFER_SIZE = MSG_BYTE_LENGTH * 1024;
    private static final int READ_STAGE_BUFFER_SIZE = MSG_BYTE_LENGTH * 1024 * 16;

    private static final int WRITE_A_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_A1_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 8;
    private static final int READ_A2_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 16;

    private static final int WRITE_AI_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_AI_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 16;

    private static final int WRITE_BODY_BUFFER_SIZE = BODY_BYTE_LENGTH * 1024;
    private static final int READ_BODY_BUFFER_SIZE = BODY_BYTE_LENGTH * 1024;

    static {
        logger.info("LsmMessageStoreImpl class loaded");
    }

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);

    private AtomicInteger threadIdCounter = new AtomicInteger(0);
    private ThreadLocal<Integer> threadIdHolder = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());
    private long[] threadMinT = new long[PRODUCER_THREAD_NUM];

    private List<DataFile> dataFileList = new ArrayList<>();
    private int dataFileCounter = 0;

    private IndexFile indexFile = new IndexFile();

    private ByteBuffer aBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);
    private ByteBuffer aiBufferForWrite = ByteBuffer.allocateDirect(WRITE_AI_BUFFER_SIZE);
    private ByteBuffer bodyBufferForWrite = ByteBuffer.allocateDirect(WRITE_BODY_BUFFER_SIZE);

    private short[] tIndex;
    private int[] tIndexSummary;
    private Map<Integer, Integer> tIndexOverflow = new HashMap<>();
    private volatile long tBase = -1;
    private long tMaxValue = Long.MIN_VALUE;
    private volatile boolean rewriteDone = false;

    private ThreadLocal<ByteBuffer> threadBufferForReadA1 = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A1_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForReadA2 = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A2_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForReadAI = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_AI_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BODY_BUFFER_SIZE));

    private List<StageFile> stageFileList = new LinkedList<>();

    public NewMessageStoreImpl() {
        for (int i = 0; i < PRODUCER_THREAD_NUM; i++) {
            StageFile stageFile = new StageFile();
            RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(DATA_DIR + "stage" + i + ".data", "rw");
            } catch (FileNotFoundException e) {
                throw new RuntimeException("no file");
            }
            stageFile.fileChannel = raf.getChannel();
            stageFile.byteBufferForWrite = ByteBuffer.allocateDirect(WRITE_STAGE_BUFFER_SIZE);
            stageFile.byteBufferForRead = ByteBuffer.allocateDirect(READ_STAGE_BUFFER_SIZE);
            stageFileList.add(stageFile);
        }
        Arrays.fill(threadMinT, -1);
    }

    @Override
    public void put(Message message) {
        long putStart = System.nanoTime();
        int putId = putCounter.getAndIncrement();
        int threadId = threadIdHolder.get();

        if (tBase < 0) {
            threadMinT[threadId] = message.getT();
            if (putId == 0) {
                _putStart = System.currentTimeMillis();
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

        if (putId == 10000 * 10000) {
            throw new RuntimeException("" + (System.currentTimeMillis() - _putStart));
        }

        stageFileList.get(threadId).writeMessage(message);

        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("Write message to stage file with t: " + message.getT() + ", a: " + message.getA()
                    + ", time: " + (System.nanoTime() - putStart) + ", putId: " + putId
            );
        }
    }

    private void rewriteFiles() {
        logger.info("Start rewrite files");
//        long rewriteStart = System.currentTimeMillis();
        int putCounter = 0;
        int currentT = 0;
        DataFile curDataFile = null;
        List<Long> aBuffer = new ArrayList<>(A_INDEX_BLOCK_SIZE);

        tMaxValue = stageFileList.stream().map(StageFile::getLastT)
                .max(Comparator.comparingLong(t -> t)).orElse(0L);
        logger.info("Determined t max value: " + tMaxValue);

        tIndex = new short[(int) (tMaxValue - tBase + 1)];
        tIndexSummary = new int[tIndex.length / T_INDEX_SUMMARY_FACTOR + 1];
        logger.info("Created t index and summary with length: " + tIndex.length);

        List<StageFile> readingFiles = new ArrayList<>(stageFileList);
        readingFiles.forEach(StageFile::prepareForRead);

        while (true) {
            int writeCount = 0;
            Iterator<StageFile> iterator = readingFiles.iterator();

            while (iterator.hasNext()) {
                StageFile stageFile = iterator.next();
                if (stageFile.doneRead) {
                    iterator.remove();
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
                        curDataFile = new DataFile(putCounter, putCounter + DATA_SEGMENT_SIZE - 1);
                        dataFileList.add(curDataFile);
                    }
                    curDataFile.writeA(message.getA());
                    curDataFile.writeBody(message.getBody());

                    aBuffer.add(message.getA());

                    if (putCounter % REWRITE_SAMPLE_RATE == 0) {
                        logger.info("Write message to data file: " + putCounter);
                    }
//                    if (putCounter == 200_000_000) {
//                        throw new RuntimeException("" + (System.currentTimeMillis() - rewriteStart) + ", " + putCounter);
//                    }
                    putCounter++;
                    writeCount++;
                }
            }

            if (readingFiles.isEmpty()) {
                break;
            }

            // update t index
            if (writeCount < Short.MAX_VALUE) {
                tIndex[currentT] = (short) writeCount;
            } else {
                tIndex[currentT] = Short.MAX_VALUE;
                tIndexOverflow.put(currentT, writeCount);
            }
            currentT++;

            // update t summary
            if (currentT % T_INDEX_SUMMARY_FACTOR == 0) {
                tIndexSummary[currentT / T_INDEX_SUMMARY_FACTOR] = putCounter;
            }

            // sort and store into a index block
            if (currentT % A_INDEX_BLOCK_SIZE == 0) {
//                long start = System.nanoTime();
                int size = aBuffer.size();
                if (size > 1) {
                    Collections.sort(aBuffer);
                }
//                if (currentT % (A_INDEX_BLOCK_SIZE * 1024) == 0) {
//                    logger.info("Sorted " + size + " a, time: " + (System.nanoTime() - start));
//                }
                long[] metaIndex = new long[(size - 1) / A_INDEX_META_FACTOR + 1 + 1];
                for (int i = 0; i < size; i++) {
                    long a = aBuffer.get(i);
                    indexFile.writeA(a);
                    if (i % A_INDEX_META_FACTOR == 0) {
                        metaIndex[i / A_INDEX_META_FACTOR] = a;
                    }
                }
                // Store max a in last element
                metaIndex[metaIndex.length - 1] = aBuffer.get(size - 1);
                indexFile.metaIndexList.add(metaIndex);
                aBuffer.clear();
            }
        }

        if (curDataFile != null) {
            curDataFile.flushABuffer();
            curDataFile.flushBodyBuffer();
        }
        indexFile.flushABuffer();
        logger.info("Done rewrite files, msg count1: " + putCounter
                + ", index list size: " + indexFile.metaIndexList.size()
                + ", discard ai: " + aBuffer.size());
    }

    private long getOffsetByTDiff(int tDiff) {
        long offset = tIndexSummary[tDiff / T_INDEX_SUMMARY_FACTOR];
        for (int i = tDiff / T_INDEX_SUMMARY_FACTOR * T_INDEX_SUMMARY_FACTOR; i < tDiff; i++) {
            int msgCount = tIndex[i];
            if (msgCount == Short.MAX_VALUE) {
                offset += tIndexOverflow.get(i);
            } else {
                offset += msgCount;
            }
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
                        totalSize += stageFile.fileSize();
                    }
                    logger.info("Flushed all stage files, total size: " + totalSize);

                    rewriteFiles();
                    rewriteDone = true;
                }
                logger.info("Rewrite task has finished, time: " + (System.currentTimeMillis() - getStart));
            }
        }

        tMax = Math.min(tMax, tMaxValue);

        ArrayList<Message> result = new ArrayList<>();
        int tDiff = (int) (tMin - tBase);
        long offset = getOffsetByTDiff(tDiff);
        long endOffset = getOffsetByTDiff((int) (tMax - tBase + 1));

        ByteBuffer aByteBufferForRead = threadBufferForReadA1.get();
        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
        aByteBufferForRead.flip();
        bodyByteBufferForRead.flip();

        for (long t = tMin; t <= tMax; t++) {
            int msgCount = tIndex[tDiff++];
            while (msgCount > 0) {
                if (!aByteBufferForRead.hasRemaining()) {
                    fillReadABuffer(aByteBufferForRead, offset, endOffset);
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    if (!bodyByteBufferForRead.hasRemaining()) {
                        fillReadBodyBuffer(bodyByteBufferForRead, offset, endOffset);
                    }
                    byte[] body = new byte[BODY_BYTE_LENGTH];
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
                    + ", aMin: " + aMin + ", aMax: " + aMax
                    + ", tRange: " + (tMax - tMin) + ", avgId: " + avgId);
            if (IS_TEST_RUN && avgId == TEST_BOUNDARY) {
                long putDuration = _putEnd - _putStart;
                long getDuration = _getEnd - _getStart;
                long avgDuration = System.currentTimeMillis() - _avgStart;
                int putScore = (int) (putCounter.get() / putDuration);
                int getScore = (int) (getMsgCounter.get() / getDuration);
                int avgScore = (int) (avgMsgCounter.get() / avgDuration);
                int totalScore = putScore + getScore + avgScore;
                logger.info("Avg result: \n"
                        + "\tread a: " + readACounter.get() + ", used a: " + usedACounter.get()
                        + ", skip a: " + skipACounter.get()
                        + ", use ratio: " + ((double) usedACounter.get() / readACounter.get())
                        + ", visit ratio: " + ((double) (usedACounter.get() + skipACounter.get()) / readACounter.get()) + "\n"
                        + "\tread ai: " + readAICounter.get() + ", used ai: " + usedAICounter.get() + ", skip ai: " + skipAICounter.get()
                        + ", jump ai: " + jump1AICounter.get() + " / " + jump2AICounter.get() + " / " + jump3AICounter.get()
                        + ", use ratio: " + ((double) usedAICounter.get() / readAICounter.get()) + "\n"
                        + ", visit ratio: " + ((double) (usedAICounter.get() + skipAICounter.get()) / readAICounter.get()) + "\n"
                        + "\tcover ratio: " + ((double) usedAICounter.get() / (usedAICounter.get() + usedACounter.get()))+ "\n"
                        + "\tfast smaller: " + fastSmallerCounter.get() + ", larger: " + fastLargerCounter.get() + "\n"
                );
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

        int tMinDiff = (int) Math.max(tMin - tBase, 0);
        int tMaxDiff = (int) (Math.min(tMax, tMaxValue) - tBase);
        if (tMaxDiff < 0) {
            return 0;
        }

        int tStart = tMinDiff - tMinDiff % A_INDEX_BLOCK_SIZE;
        if (tMinDiff != tStart) {
            tStart += A_INDEX_BLOCK_SIZE;
        }
        int tEnd = (tMaxDiff + 1) / A_INDEX_BLOCK_SIZE * A_INDEX_BLOCK_SIZE; // exclusive
//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            int blocks = (tEnd - tStart) / A_INDEX_BLOCK_SIZE;
//            logger.info("tStart: " + tStart + ", tEnd: " + tEnd
//                    + ", blocks: " + blocks + ", covered: " + blocks * A_INDEX_BLOCK_SIZE
//                    + ", uncovered: " + (tMaxDiff - tMinDiff - tEnd + tStart) + ", avgId: " + avgId);
//        }
//        int avgId = 0;

        if (tStart >= tEnd) {
            // Back to normal
            SumAndCount result = getAvgValueNormal(avgId, aMin, aMax, tMinDiff, tMaxDiff);
            sum += result.sum;
            count += result.count;

        } else {
            // Process head
            if (tMinDiff != tStart) {
                SumAndCount result = getAvgValueNormal(avgId, aMin, aMax, tMinDiff, tStart - 1);
                sum += result.sum;
                count += result.count;
            }
            // Process tail
            if (tMaxDiff > tEnd - 1) {
                SumAndCount result = getAvgValueNormal(avgId, aMin, aMax, tEnd, tMaxDiff);
                sum += result.sum;
                count += result.count;
            }
            // Process middle
            for (int t = tStart; t < tEnd; t += A_INDEX_BLOCK_SIZE) {
                SumAndCount result = getAvgValueFast(avgId, aMin, aMax, t, t + A_INDEX_BLOCK_SIZE - 1);
                sum += result.sum;
                count += result.count;
            }
        }

        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("Got " + count + ", time: " + (System.currentTimeMillis() - avgStart) + ", avgId: " + avgId);
        }
        if (IS_TEST_RUN) {
            avgMsgCounter.addAndGet(count);
        }
        return count > 0 ? sum / count : 0;
    }

    private SumAndCount getAvgValueNormal(int avgId, long aMin, long aMax, int tMin, int tMax) {
//        long avgStart = System.nanoTime();
        long sum = 0;
        int read = 0;
        int count = 0;
        int skip = 0;

        long offset = getOffsetByTDiff(tMin);
        long endOffset = getOffsetByTDiff(tMax + 1);

        ByteBuffer aByteBufferForRead = threadBufferForReadA2.get();
        aByteBufferForRead.flip();

        for (int t = tMin; t <= tMax; t++) {
            int msgCount = tIndex[t];
            if (msgCount == Short.MAX_VALUE) {
                msgCount = tIndexOverflow.get(t);
            }
            while (msgCount > 0) {
                if (aByteBufferForRead.remaining() == 0) {
                    read += fillReadABuffer(aByteBufferForRead, offset, endOffset);
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                } else {
                    skip++;
                }
                offset++;
                msgCount--;
            }
        }
        aByteBufferForRead.clear();

//        readACounter.addAndGet(read / KEY_A_BYTE_LENGTH);
//        usedACounter.addAndGet(count);
//        skipACounter.addAndGet(skip);
//
//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("Normal got " + count + ", skip: " + skip
//                    + ", time: " + (System.nanoTime() - avgStart) + ", avgId: " + avgId);
//        }
        return new SumAndCount(sum, count);
    }

    private SumAndCount getAvgValueFast(int avgId, long aMin, long aMax, int tMin, int tMax) {
//        long avgStart = System.nanoTime();
        long sum = 0;
        int read = 0;
        int count = 0;
        int skip = 0;
        int jump1 = 0;
        int jump2 = 0;
        int jump3 = 0;

        long[] metaIndex = indexFile.metaIndexList.get(tMin / A_INDEX_BLOCK_SIZE);
        if (metaIndex[0] > aMax) {
            fastSmallerCounter.addAndGet(A_INDEX_BLOCK_SIZE);
            return new SumAndCount(0, 0);
        }
        if (metaIndex[metaIndex.length - 1] < aMin) {
            fastLargerCounter.addAndGet(A_INDEX_BLOCK_SIZE);
            return new SumAndCount(0, 0);
        }

        long fullStartOffset = getOffsetByTDiff(tMin);
        long fullEndOffset = getOffsetByTDiff(tMax + 1);
        long startOffset = fullStartOffset;
        long endOffset = fullEndOffset;

        // binary search for start
        int start = 0;
        if (metaIndex[0] < aMin) {
            int end = metaIndex.length - 2;
            while (start < end) {
                int index = (start + end) / 2;
                if (metaIndex[index] < aMin) {
                    start = index + 1;
                } else {
                    end = index;
                }
            }
            start = Math.max(start - 1, 0);
            startOffset += start * A_INDEX_META_FACTOR;
        }

        // binary search for end
        if (metaIndex[metaIndex.length - 2] > aMax) {
            int end = metaIndex.length - 2;
            while (start < end) {
                int index = (start + end) / 2;
                if (metaIndex[index] <= aMax) {
                    start = index + 1;
                } else {
                    end = index;
                }
            }
            if (end < metaIndex.length - 2) {
                endOffset = fullStartOffset + end * A_INDEX_META_FACTOR;
            }
        }

//        jump1 = (int) (startOffset - fullStartOffset);
//        jump2 = (int) (fullEndOffset - endOffset);

        ByteBuffer aiByteBufferForRead = threadBufferForReadAI.get();
        aiByteBufferForRead.flip();

        for (long offset = startOffset; offset < endOffset; offset++) {
            if (aiByteBufferForRead.remaining() == 0) {
                read += fillReadAIBuffer(aiByteBufferForRead, offset, endOffset);
            }
            long a = aiByteBufferForRead.getLong();
            if (a > aMax) {
                jump3 += (endOffset - offset - 1);
                break;
            }
            if (a >= aMin) {
                sum += a;
                count++;
            }
//            else {
//                skip++;
//            }
        }
        aiByteBufferForRead.clear();

//        readAICounter.addAndGet(read / KEY_A_BYTE_LENGTH);
//        usedAICounter.addAndGet(count);
//        skipAICounter.addAndGet(skip);
//        jump1AICounter.addAndGet(jump1);
//        jump2AICounter.addAndGet(jump2);
//        jump3AICounter.addAndGet(jump3);
//
//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("Fast got " + count + ", skip: " + skip + ", jump: " + jump1 + "/" + jump2 + "/" + jump3
//                    + ", time: " + (System.nanoTime() - avgStart) + ", avgId: " + avgId);
//        }
        return new SumAndCount(sum, count);
    }

    public static byte[] longToBytes(long l) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return result;
    }

    public static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    private class StageFile {
        FileChannel fileChannel;
        ByteBuffer byteBufferForWrite;
        ByteBuffer byteBufferForRead;
        long fileOffset;
        Message peeked;
        boolean doneRead;
        byte[] result = new byte[8];

        void writeMessage(Message message) {
            if (!byteBufferForWrite.hasRemaining()) {
                flushBuffer();
            }
            int tDiff = (int) (message.getT() - tBase);
            byteBufferForWrite.putInt(tDiff);
            long a = message.getA();
            for (int i = 7; i >= 0; i--) {
                result[i] = (byte)(a & 0xFF);
                a >>= 8;
            }
            byteBufferForWrite.put(result);
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
            return byteBuffer.getInt() + tBase;
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

        boolean fillReadBuffer() {
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
    }

    private int fillReadABuffer(ByteBuffer readABuffer, long offset, long endOffset) {
        DataFile dataFile = dataFileList.get((int) (offset / DATA_SEGMENT_SIZE));
        return fillReadBuffer(readABuffer, dataFile.aChannel, offset - dataFile.start,
                endOffset - dataFile.start, KEY_A_BYTE_LENGTH);
    }

    private int fillReadAIBuffer(ByteBuffer readAIBuffer, long offset, long endOffset) {
        return fillReadBuffer(readAIBuffer, indexFile.aiChannel, offset, endOffset, KEY_A_BYTE_LENGTH);
    }

    private void fillReadBodyBuffer(ByteBuffer readBodyBuffer, long offset, long endOffset) {
        DataFile dataFile = dataFileList.get((int) (offset / DATA_SEGMENT_SIZE));
        fillReadBuffer(readBodyBuffer, dataFile.bodyChannel, offset - dataFile.start,
                endOffset - dataFile.start, BODY_BYTE_LENGTH);
    }

    private int fillReadBuffer(ByteBuffer readBuffer, FileChannel fileChannel,
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

    private void writeLong(long value, ByteBuffer byteBuffer, FileChannel fileChannel) {
        if (!byteBuffer.hasRemaining()) {
            flushBuffer(byteBuffer, fileChannel);
        }
        byteBuffer.putLong(value);
    }

    private void writeBytes(byte[] bytes, ByteBuffer byteBuffer, FileChannel fileChannel) {
        if (!byteBuffer.hasRemaining()) {
            flushBuffer(byteBuffer, fileChannel);
        }
        byteBuffer.put(bytes);
    }

    private void flushBuffer(ByteBuffer byteBuffer, FileChannel fileChannel) {
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException("write error");
        }
        byteBuffer.clear();
    }

    private class DataFile {
        int index;
        int start;
        int end;
        RandomAccessFile aFile;
        RandomAccessFile asFile;
        RandomAccessFile bodyFile;
        FileChannel aChannel;
        FileChannel asChannel;
        FileChannel bodyChannel;

        DataFile(int start, int end) {
            this.start = start;
            this.end = end;
            if (this.end < 0) {
                this.end = MSG_COUNT_UPPER_LIMIT;
            }
            this.index = dataFileCounter++;
            try {
                this.aFile = new RandomAccessFile(
                        DATA_DIR + "a" + this.index + ".data", "rw");
                this.asFile = new RandomAccessFile(
                        DATA_DIR + "as" + this.index + ".data", "rw");
                this.bodyFile = new RandomAccessFile(
                        DATA_DIR + "body" + this.index + ".data", "rw");
            } catch (FileNotFoundException e) {
                throw new RuntimeException("file error");
            }
            this.aChannel = this.aFile.getChannel();
            this.asChannel = this.asFile.getChannel();
            this.bodyChannel = this.bodyFile.getChannel();
//            logger.info("Created data file: [" + start + ", " + end + "]");
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
    }

    private class IndexFile {
        RandomAccessFile aiFile;
        FileChannel aiChannel;
        List<long[]> metaIndexList;

        IndexFile() {
            try {
                this.aiFile = new RandomAccessFile(
                        DATA_DIR + "ai.data", "rw");
            } catch (FileNotFoundException e) {
                throw new RuntimeException("file error");
            }
            this.aiChannel = this.aiFile.getChannel();
            metaIndexList = new ArrayList<>(1024 * 1024);
        }

        void writeA(long a) {
            writeLong(a, aiBufferForWrite, aiChannel);
        }

        void flushABuffer() {
            flushBuffer(aiBufferForWrite, aiChannel);
        }
    }

    private static class SumAndCount {
        long sum;
        long count;

        SumAndCount(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }
    }

    private static final int PUT_SAMPLE_RATE = 100000000;
    private static final int REWRITE_SAMPLE_RATE = 100000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);

    private AtomicInteger readACounter  = new AtomicInteger(0);
    private AtomicInteger usedACounter  = new AtomicInteger(0);
    private AtomicInteger skipACounter = new AtomicInteger(0);
    private AtomicInteger readAICounter  = new AtomicInteger(0);
    private AtomicInteger usedAICounter  = new AtomicInteger(0);
    private AtomicInteger skipAICounter  = new AtomicInteger(0);
    private AtomicInteger jump1AICounter  = new AtomicInteger(0);
    private AtomicInteger jump2AICounter  = new AtomicInteger(0);
    private AtomicInteger jump3AICounter  = new AtomicInteger(0);
    private AtomicInteger fastSmallerCounter = new AtomicInteger(0);
    private AtomicInteger fastLargerCounter = new AtomicInteger(0);

    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;
}
