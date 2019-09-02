package io.openmessaging;


import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
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

    private short[] tIndex = null;
    private int[] tIndexSummary = null;
    private Map<Integer, Integer> tIndexOverflow = new HashMap<>();
    private volatile long tBase = -1;
    private long tMaxValue = Long.MIN_VALUE;
    private volatile boolean rewriteDone = false;

    private StageFile[] stageFileList = new StageFile[PRODUCER_THREAD_NUM];
    private DataFile dataFile = new DataFile();
    private IndexFile mainIndexFile = new IndexFile();
    private IndexFile subIndexFile = new IndexFile();

    private ThreadLocal<ByteBuffer> threadBufferForReadA1 = createDirectBuffer(READ_A1_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadA2 = createDirectBuffer(READ_A2_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAIM = createDirectBuffer(READ_AIM_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAIS = createDirectBuffer(READ_AIS_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = createDirectBuffer(READ_BODY_BUFFER_SIZE);

    private ThreadPoolExecutor aimIndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor aisIndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    public DefaultMessageStoreImpl() {
        for (int i = 0; i < stageFileList.length; i++) {
            StageFile stageFile = new StageFile(i);
            stageFileList[i] = stageFile;
        }
        Arrays.fill(threadMinT, -1);
    }

    @Override
    public void put(Message message) {
//        long putStart = System.nanoTime();
        int threadId = threadIdHolder.get();

        if (tBase < 0) {
            threadMinT[threadId] = message.getT();
            int putId = putCounter.getAndIncrement();
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
//        if (putId == 20000 * 10000) {
//            throw new RuntimeException("" + (System.currentTimeMillis() - PerfStats._putStart));
//        }
        stageFileList[threadId].writeMessage(message);

//        if (putId % PUT_SAMPLE_RATE == 0) {
//            logger.info("Write message to stage file with t: " + message.getT() + ", a: " + message.getA()
//                    + ", time: " + (System.nanoTime() - putStart) + ", putId: " + putId);
//        }
    }

    private int rewriteFiles() {
        logger.info("Start rewrite files");
        long rewriteStart = System.currentTimeMillis();
        int rewriteCount = 0;
        int currentT = 0;
        List<Long> amBuffer = new ArrayList<>(A_INDEX_MAIN_BLOCK_SIZE);
        List<Long> asBuffer = new ArrayList<>(A_INDEX_SUB_BLOCK_SIZE);
        AtomicInteger pendingTasks = new AtomicInteger();

        tMaxValue = Arrays.stream(stageFileList).mapToLong(StageFile::getLastT).max().orElse(0);
        logger.info("Determined t max value: " + tMaxValue);

        tIndex = new short[(int) (tMaxValue - tBase + 1)];
        tIndexSummary = new int[tIndex.length / T_INDEX_SUMMARY_FACTOR + 1];
        logger.info("Created t index and summary with length: " + tIndex.length);

        Arrays.stream(stageFileList).forEach(StageFile::prepareForRead);

        while (true) {
            int repeatCount = 0;
            int doneCount = 0;
            for (StageFile stageFile : stageFileList) {
                if (stageFile.isDoneRead()) {
                    doneCount++;
                    continue;
                }
                Message head;
                while ((head = stageFile.peekMessage()) != null && head.getT() == currentT + tBase) {
                    Message message = stageFile.consumePeeked();
                    dataFile.writeA(message.getA());
                    dataFile.writeBody(message.getBody());

                    amBuffer.add(message.getA());
                    asBuffer.add(message.getA());

//                    if (rewriteCount % REWRITE_SAMPLE_RATE == 0) {
//                        logger.info("Write message to data file: " + rewriteCount);
//                    }
//                    if (putCounter == 200_000_000) {
//                        throw new RuntimeException("" + (System.currentTimeMillis() - rewriteStart) + ", " + putCounter);
//                    }
                    rewriteCount++;
                    repeatCount++;
                }
            }

            if (doneCount == stageFileList.length) {
                logger.info("All stage files have been read");
                break;
            }

            // update t index
            if (repeatCount < Short.MAX_VALUE) {
                tIndex[currentT] = (short) repeatCount;
            } else {
                tIndex[currentT] = Short.MAX_VALUE;
                tIndexOverflow.put(currentT, repeatCount);
            }
            currentT++;

            // update t summary
            if (currentT % T_INDEX_SUMMARY_FACTOR == 0) {
                tIndexSummary[currentT / T_INDEX_SUMMARY_FACTOR] = rewriteCount;
            }

            // sort and store into a index main block
            if (currentT % A_INDEX_MAIN_BLOCK_SIZE == 0) {
                List<Long> finalAmBuffer = amBuffer;
                pendingTasks.incrementAndGet();
                aimIndexWriter.execute(() -> {
                    int size = finalAmBuffer.size();
                    if (size > 1) {
                        Collections.sort(finalAmBuffer);
                    }
//                    long sum = 0;
                    long[] metaIndex = new long[(size - 1) / A_INDEX_META_FACTOR + 1];
                    for (int i = 0; i < size; i++) {
                        long a = finalAmBuffer.get(i);
//                        sum += a;
                        mainIndexFile.writeA(a);
//                        mainIndexFile.writeA(sum);
                        if (i % A_INDEX_META_FACTOR == 0) {
                            metaIndex[i / A_INDEX_META_FACTOR] = a;
                        }
                    }
                    mainIndexFile.addMetaIndex(metaIndex);
                    mainIndexFile.addRangeMax(finalAmBuffer.get(size - 1));
//                    mainIndexFile.addRangeSum(sum);
                    pendingTasks.decrementAndGet();
                });
                amBuffer = new ArrayList<>(A_INDEX_MAIN_BLOCK_SIZE);
            }

            // sort and store into a index sub block
            if (currentT % A_INDEX_SUB_BLOCK_SIZE == 0) {
                List<Long> finalAsBuffer = asBuffer;
                pendingTasks.incrementAndGet();
                aisIndexWriter.execute(() -> {
                    int size = finalAsBuffer.size();
                    if (size > 1) {
                        Collections.sort(finalAsBuffer);
                    }
                    long[] metaIndex = new long[(size - 1) / A_INDEX_META_FACTOR + 1];
                    for (int i = 0; i < size; i++) {
                        long a = finalAsBuffer.get(i);
                        subIndexFile.writeA(a);
                        if (i % A_INDEX_META_FACTOR == 0) {
                            metaIndex[i / A_INDEX_META_FACTOR] = a;
                        }
                    }
                    subIndexFile.addMetaIndex(metaIndex);
                    subIndexFile.addRangeMax(finalAsBuffer.get(size - 1));
                    pendingTasks.decrementAndGet();
                });
                asBuffer = new ArrayList<>(A_INDEX_SUB_BLOCK_SIZE);
            }
        }

        while (pendingTasks.get() > 0) {
            logger.info("Waiting for a main/sub index file writer to finish");
            LockSupport.parkNanos(10_000_000);
        }
        aimIndexWriter.shutdown();
        aisIndexWriter.shutdown();
        logger.info("All a index file writer has finished");

        dataFile.flushABuffer();
        dataFile.flushBodyBuffer();
        mainIndexFile.flushABuffer();
        subIndexFile.flushABuffer();

        stageFileList = null;
        System.gc();
        logger.info("Done rewrite files, rewrite count: " + rewriteCount
                + ", time: " + (System.currentTimeMillis() - rewriteStart));
        return rewriteCount;
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        long getStart = System.currentTimeMillis();
//        int getId = getCounter.getAndIncrement();
//        if (getId == 0) {
//            PerfStats._putEnd = System.currentTimeMillis();
//            PerfStats._getStart = PerfStats._putEnd;
////            PerfStats.printStats(this);
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
                    logger.info("Flushed all stage files, total size: " + totalSize
                            + ", file count: " + totalSize / STAGE_MSG_BYTE_LENGTH);
                    int rewriteCount;
                    try {
                        rewriteCount = rewriteFiles();
                    } catch (Throwable e) {
                        logger.info("Failed to rewrite files: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                        throw e;
                    }
                    if (rewriteCount != totalSize / STAGE_MSG_BYTE_LENGTH) {
                        logger.info("Inconsistent msg count");
                        throw new RuntimeException("Abort!");
                    }
                    rewriteDone = true;
                }
                logger.info("Rewrite task has finished" +
                        ", t index overflow: " + tIndexOverflow.size() +
                        ", time: " + (System.currentTimeMillis() - _getStart));
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

//        if (getId % GET_SAMPLE_RATE == 0) {
//            logger.info("Return sorted result with size: " + result.size()
//                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
//        }
//        getMsgCounter.addAndGet(result.size());

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
        long sum = 0;
        int count = 0;

        int tMinDiff = (int) Math.max(tMin - tBase, 0);
        int tMaxDiff = (int) (Math.min(tMax, tMaxValue) - tBase);
        if (tMaxDiff < 0) {
            return 0;
        }

        int tStart = tMinDiff - tMinDiff % A_INDEX_SUB_BLOCK_SIZE;
        if (tMinDiff != tStart) {
            tStart += A_INDEX_SUB_BLOCK_SIZE;
        }
        // exclusive
        int tEnd = (tMaxDiff + 1) / A_INDEX_SUB_BLOCK_SIZE * A_INDEX_SUB_BLOCK_SIZE;

        SumAndCount result;

        if (tStart >= tEnd) {
            // Back to normal
            result = getAvgValueFromDataFile(aMin, aMax, tMinDiff, tMaxDiff);
            sum += result.getSum();
            count += result.getCount();

        } else {
            // Process head
            if (tMinDiff != tStart) {
                result = getAvgValueFromDataFile(aMin, aMax, tMinDiff, tStart - 1);
                sum += result.getSum();
                count += result.getCount();
            }
            // Process tail
            if (tMaxDiff > tEnd - 1) {
                result = getAvgValueFromDataFile(aMin, aMax, tEnd, tMaxDiff);
                sum += result.getSum();
                count += result.getCount();
            }
            // Process middle
            int t = tStart;
            while (t < tEnd) {
                if (t % A_INDEX_MAIN_BLOCK_SIZE == 0 && t + A_INDEX_MAIN_BLOCK_SIZE <= tEnd) {
                    result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_MAIN_BLOCK_SIZE - 1, true);
//                    result = getAvgValueFromAccumIndex(aMin, aMax, t, t + A_INDEX_MAIN_BLOCK_SIZE - 1, true);
                    t += A_INDEX_MAIN_BLOCK_SIZE;
                } else {
                    result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_SUB_BLOCK_SIZE - 1, false);
                    t += A_INDEX_SUB_BLOCK_SIZE;
                }
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

    private SumAndCount getAvgValueFromDataFile(long aMin, long aMax, int tMin, int tMax) {
        long sum = 0;
        int count = 0;

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
                    dataFile.fillReadABuffer(aByteBufferForRead, offset, endOffset);
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }
                offset++;
                msgCount--;
            }
        }
        aByteBufferForRead.clear();

        return new SumAndCount(sum, count);
    }

    private SumAndCount getAvgFromSortedIndex(long aMin, long aMax, int tMin, int tMax, boolean isMain) {
        long sum = 0;
        int count = 0;

        IndexFile indexFile = isMain ? mainIndexFile : subIndexFile;
        int blockSize = isMain ? A_INDEX_MAIN_BLOCK_SIZE : A_INDEX_SUB_BLOCK_SIZE;
        ByteBuffer aiByteBufferForRead = isMain ? threadBufferForReadAIM.get() : threadBufferForReadAIS.get();

        long[] metaIndex = indexFile.getMetaIndex(tMin / blockSize);
        long rangeMin = metaIndex[0];
        long rangeMax = indexFile.getRangeMax(tMin / blockSize);

        if (rangeMin > aMax) {
            return new SumAndCount(0, 0);
        } else if (rangeMax < aMin) {
            return new SumAndCount(0, 0);
        }

        long fullStartOffset = getOffsetByTDiff(tMin);
        long fullEndOffset = getOffsetByTDiff(tMax + 1);

        long startOffset = fullStartOffset;
        long endOffset = fullEndOffset;

        // binary search for start
        int start = 0;
        if (rangeMin < aMin) {
            int end = metaIndex.length - 1;
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
        if (metaIndex[metaIndex.length - 1] > aMax) {
            int end = metaIndex.length - 1;
            while (start < end) {
                int index = (start + end) / 2;
                if (metaIndex[index] <= aMax) {
                    start = index + 1;
                } else {
                    end = index;
                }
            }
            if (end < metaIndex.length - 1) {
                endOffset = fullStartOffset + end * A_INDEX_META_FACTOR;
            }
        }

        aiByteBufferForRead.flip();

        for (long offset = startOffset; offset < endOffset; offset++) {
            if (aiByteBufferForRead.remaining() == 0) {
                indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, endOffset);
            }
            long a = aiByteBufferForRead.getLong();
            if (a > aMax) {
                break;
            }
            if (a >= aMin) {
                sum += a;
                count++;
            }
        }

        aiByteBufferForRead.clear();

        return new SumAndCount(sum, count);
    }

    private SumAndCount getAvgValueFromAccumIndex(long aMin, long aMax, int tMin, int tMax, boolean isMain) {
        long sum;
        int count;

        IndexFile indexFile = isMain ? mainIndexFile : subIndexFile;
        int blockSize = isMain ? A_INDEX_MAIN_BLOCK_SIZE : A_INDEX_SUB_BLOCK_SIZE;
        ByteBuffer aiByteBufferForRead = isMain ? threadBufferForReadAIM.get() : threadBufferForReadAIS.get();

        long[] metaIndex = indexFile.getMetaIndex(tMin / blockSize);
        long rangeMin = metaIndex[0];
        long rangeMax = indexFile.getRangeMax(tMin / blockSize);
        long rangeSum = indexFile.getRangeSum(tMin / blockSize);

        if (aMax < rangeMin) {
            return new SumAndCount(0, 0);
        } else if (aMin > rangeMax) {
            return new SumAndCount(0, 0);
        }

        long fullStartOffset = getOffsetByTDiff(tMin);
        long fullEndOffset = getOffsetByTDiff(tMax + 1);

        if (aMin <= rangeMin && aMax >= rangeMax) {
            sum = rangeSum;
            count = (int) (fullEndOffset - fullStartOffset);
            return new SumAndCount(sum, count);
        }

        long startOffset = fullStartOffset;
        long endOffset = fullEndOffset;
        long realStartOffset = -1;
        long realEndOffset = -1;

        int start = 0;
        if (aMin > rangeMin) {
            // binary search for start offset which < aMin
            int end = metaIndex.length - 1;
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
        } else {
            realStartOffset = fullStartOffset;
        }

        if (aMax < rangeMax) {
            // binary search for end offset (<= aMax)
            int end = metaIndex.length - 1;
            while (start < end - 1) {
                int index = (start + end) / 2;
                if (metaIndex[index] <= aMax) {
                    start = index;
                } else {
                    end = index - 1;
                }
            }
            endOffset = fullStartOffset + start * A_INDEX_META_FACTOR;
        } else {
            realEndOffset = fullEndOffset;
        }

        long startPrevSum = 0;
        long endSum = rangeSum;

        // need to find real first offset
        if (realStartOffset < 0) {
            aiByteBufferForRead.flip();
            long prevSum = 0;
            for (long offset = startOffset; offset < fullEndOffset; offset++) {
                if (aiByteBufferForRead.remaining() == 0) {
                    indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, fullEndOffset);
                }
                long aSum = aiByteBufferForRead.getLong();
                // first one should not be in range (< aMin)
                if (offset > startOffset) {
                    long a = aSum - prevSum;
                    if (a >= aMin) {
                        realStartOffset = offset;
                        startPrevSum = prevSum;
                        break;
                    }
                }
                prevSum = aSum;
            }
            aiByteBufferForRead.clear();
        }

        // need to find real last offset
        if (realEndOffset < 0) {
            aiByteBufferForRead.flip();
            long prevSum = 0;
            for (long offset = endOffset; offset < fullEndOffset; offset++) {
                if (aiByteBufferForRead.remaining() == 0) {
                    indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, fullEndOffset);
                }
                long aSum = aiByteBufferForRead.getLong();
                // first one should not be in range (<= aMax)
                if (offset > endOffset) {
                    long a = aSum - prevSum;
                    if (a > aMax) {
                        realEndOffset = offset;
                        endSum = prevSum;
                        break;
                    }
                }
                prevSum = aSum;
            }
            aiByteBufferForRead.clear();
        }

        sum = endSum - startPrevSum;
        count = (int) (realEndOffset - realStartOffset);

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
