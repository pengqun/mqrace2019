package io.openmessaging;


import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static io.openmessaging.CommonUtils.createDirectBuffer;
import static io.openmessaging.Constants.*;
import static io.openmessaging.PerfStats.*;

/**
 * @author pengqun.pq
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(DefaultMessageStoreImpl.class);

    static {
        logger.info("LsmMessageStoreImpl class loaded");
    }

    private AtomicInteger putCounter = new AtomicInteger();
    private AtomicInteger getCounter = new AtomicInteger();
    private AtomicInteger avgCounter = new AtomicInteger();

    private AtomicInteger threadIdCounter = new AtomicInteger();
    private ThreadLocal<Integer> threadIdHolder = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());
    private long[] threadMinT = new long[PRODUCER_THREAD_NUM];
    private long[] threadMaxT = new long[PRODUCER_THREAD_NUM];

    private short[] tIndex;
    private int[] tIndexSummary;
    private Map<Integer, Integer> tIndexOverflow = new HashMap<>();
    private volatile long tBase = -1;
    private long tMaxValue = Long.MIN_VALUE;
    private volatile boolean rewriteDone = false;

    private List<StageFile> stageFileList = new ArrayList<>();
    private ThreadLocal<StageFile> threadStageFile = ThreadLocal.withInitial(() -> {
        StageFile stageFile = new StageFile(threadIdHolder.get());
        stageFileList.add(stageFile);
        return stageFile;
    });

    private DataFile dataFile = new DataFile();
    private IndexFile indexFile = new IndexFile();

    private ThreadLocal<ByteBuffer> threadBufferForReadA1 = createDirectBuffer(READ_A1_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadA2 = createDirectBuffer(READ_A2_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAI = createDirectBuffer(READ_AI_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = createDirectBuffer(READ_BODY_BUFFER_SIZE);

    private ThreadPoolExecutor aiIndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    public DefaultMessageStoreImpl() {
        Arrays.fill(threadMinT, -1);
        Arrays.fill(threadMaxT, -1);
    }

    @Override
    public void put(Message message) {
//        long putStart = System.nanoTime();
        int putId = putCounter.getAndIncrement();

        if (tBase < 0) {
            threadMinT[threadIdHolder.get()] = message.getT();
            if (putId == 0) {
                PerfStats._putStart = System.currentTimeMillis();
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

        if (putId == 20000 * 10000) {
            throw new RuntimeException("" + (System.currentTimeMillis() - PerfStats._putStart));
        }

        threadStageFile.get().writeMessage(message);

        threadMaxT[threadIdHolder.get()] = message.getT();

//        if (putId % PUT_SAMPLE_RATE == 0) {
//            logger.info("Write message to stage file with t: " + message.getT() + ", a: " + message.getA()
//                    + ", time: " + (System.nanoTime() - putStart) + ", putId: " + putId);
//        }
    }

    private void rewriteFiles() {
        logger.info("Start rewrite files");
        long rewriteStart = System.currentTimeMillis();
        int putCounter = 0;
        int currentT = 0;
        List<Long> aBuffer = new ArrayList<>(A_INDEX_BLOCK_SIZE);
        AtomicInteger pending = new AtomicInteger();

        tMaxValue = Arrays.stream(threadMaxT).max().orElse(tMaxValue);
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
                if (stageFile.isDoneRead()) {
                    iterator.remove();
                }
                Message head;
                while ((head = stageFile.peekMessage()) != null && head.getT() == currentT) {
                    // Got ordered message
                    Message message = stageFile.consumePeeked();
                    dataFile.writeA(message.getA());
                    dataFile.writeBody(message.getBody());

                    aBuffer.add(message.getA());

//                    if (putCounter % REWRITE_SAMPLE_RATE == 0) {
//                        logger.info("Write message to data file: " + putCounter);
//                    }
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
                List<Long> finalABuffer = aBuffer;
                pending.incrementAndGet();
                aiIndexWriter.execute(() -> {
                    int size = finalABuffer.size();
                    if (size > 1) {
                        Collections.sort(finalABuffer);
                    }
                    long[] metaIndex = new long[(size - 1) / A_INDEX_META_FACTOR + 1 + 1];
                    for (int i = 0; i < size; i++) {
                        long a = finalABuffer.get(i);
                        indexFile.writeA(a);
                        if (i % A_INDEX_META_FACTOR == 0) {
                            metaIndex[i / A_INDEX_META_FACTOR] = a;
                        }
                    }
                    // Store max a in last element
                    metaIndex[metaIndex.length - 1] = finalABuffer.get(size - 1);
                    indexFile.getMetaIndexList().add(metaIndex);
                    pending.decrementAndGet();
                });
                aBuffer = new ArrayList<>(A_INDEX_BLOCK_SIZE);
            }
        }

        dataFile.flushABuffer();
        dataFile.flushBodyBuffer();

        while (pending.get() > 0) {
            logger.info("Waiting for index file writing done");
            LockSupport.parkNanos(100_000_000);
        }
        indexFile.flushABuffer();
        aiIndexWriter.shutdown();

        logger.info("Done rewrite files, msg count: " + putCounter
                + ", index list size: " + indexFile.getMetaIndexList().size()
                + ", discard ai: " + aBuffer.size());
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        long getStart = System.currentTimeMillis();
//        int getId = getCounter.getAndIncrement();
//        if (getId == 0) {
//            PerfStats._putEnd = System.currentTimeMillis();
//            PerfStats._getStart = PerfStats._putEnd;
//            PerfStats.printStats(this);
//        }
//        if (getId % GET_SAMPLE_RATE == 0) {
//            logger.info("getMessage - tMin: " + tMin + ", tMax: " + tMax
//                    + ", aMin: " + aMin + ", aMax: " + aMax + ", getId: " + getId);
//        }
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
                logger.info("Rewrite task has finished, time: " + (System.currentTimeMillis() - _getStart));
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
                    dataFile.fillReadABuffer(aByteBufferForRead, offset, endOffset);
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    if (!bodyByteBufferForRead.hasRemaining()) {
                        dataFile.fillReadBodyBuffer(bodyByteBufferForRead, offset, endOffset);
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

//        getMsgCounter.addAndGet(result.size());
//
//        if (getId % GET_SAMPLE_RATE == 0) {
//            logger.info("Return sorted result with size: " + result.size()
//                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
//        }
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        long avgStart = System.currentTimeMillis();
//        int avgId = avgCounter.getAndIncrement();
//        if (avgId == 0) {
//            PerfStats._getEnd = System.currentTimeMillis();
//            PerfStats._avgStart = PerfStats._getEnd;
//        }
//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("getAvgValue - tMin: " + tMin + ", tMax: " + tMax + ", aMin: " + aMin + ", aMax: " + aMax
//                    + ", tRange: " + (tMax - tMin) + ", avgId: " + avgId);
//        }
//        if (avgId == TEST_BOUNDARY) {
//            PerfStats.printStats(this);
//        }
        int avgId = 0;

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
            sum += result.getSum();
            count += result.getCount();

        } else {
            // Process head
            if (tMinDiff != tStart) {
                SumAndCount result = getAvgValueNormal(avgId, aMin, aMax, tMinDiff, tStart - 1);
                sum += result.getSum();
                count += result.getCount();
            }
            // Process tail
            if (tMaxDiff > tEnd - 1) {
                SumAndCount result = getAvgValueNormal(avgId, aMin, aMax, tEnd, tMaxDiff);
                sum += result.getSum();
                count += result.getCount();
            }
            // Process middle
            for (int t = tStart; t < tEnd; t += A_INDEX_BLOCK_SIZE) {
                SumAndCount result = getAvgValueFast(avgId, aMin, aMax, t, t + A_INDEX_BLOCK_SIZE - 1);
                sum += result.getSum();
                count += result.getCount();
            }
        }

//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("Got " + count + ", time: " + (System.currentTimeMillis() - avgStart) + ", avgId: " + avgId);
//        }
//        avgMsgCounter.addAndGet(count);

        return count > 0 ? sum / count : 0;
    }

    private SumAndCount getAvgValueNormal(int avgId, long aMin, long aMax, int tMin, int tMax) {
//        long avgStart = System.nanoTime();
        long sum = 0;
        int count = 0;
//        int read = 0;
//        int skip = 0;

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
//                    read +=
                    dataFile.fillReadABuffer(aByteBufferForRead, offset, endOffset);
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }
//                else {
//                    skip++;
//                }
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
        int count = 0;
//        int read = 0;
//        int skip = 0;
//        int jump1 = 0;
//        int jump2 = 0;
//        int jump3 = 0;

        long[] metaIndex = indexFile.getMetaIndexList().get(tMin / A_INDEX_BLOCK_SIZE);
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
//                read +=
                indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, endOffset);
            }
            long a = aiByteBufferForRead.getLong();
            if (a > aMax) {
//                jump3 += (endOffset - offset - 1);
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

    AtomicInteger getPutCounter() {
        return putCounter;
    }
}
