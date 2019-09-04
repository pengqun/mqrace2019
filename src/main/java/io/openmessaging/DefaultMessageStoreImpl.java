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

/**
 * @author pengqun.pq
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(DefaultMessageStoreImpl.class);

    private AtomicInteger putCounter = new AtomicInteger();
    private AtomicInteger threadIdCounter = new AtomicInteger();
    private ThreadLocal<Integer> threadIdHolder = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());
    private long[] threadMinT = new long[PRODUCER_THREAD_NUM];

    private short[] tIndex = null;
    private int[] tIndexSummary = null;
    private Map<Integer, Integer> tIndexOverflow = new HashMap<>();

    private volatile long tBase = Long.MIN_VALUE;
    private long tMaxValue = Long.MIN_VALUE;
    private volatile boolean rewriteDone = false;
    private AtomicInteger pendingTasks = new AtomicInteger();

    private StageFile[] stageFileList = new StageFile[PRODUCER_THREAD_NUM];
    private DataFile dataFile = new DataFile();
    private IndexFile level1IndexFile = new IndexFile(1);
    private IndexFile level2IndexFile = new IndexFile(2);
    private IndexFile level3IndexFile = new IndexFile(3);

    private ThreadLocal<ByteBuffer> threadBufferForReadA1 = createDirectBuffer(READ_A1_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadA2 = createDirectBuffer(READ_A2_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAiL1 = createDirectBuffer(READ_AI_L1_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAiL2 = createDirectBuffer(READ_AI_L2_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAiL3 = createDirectBuffer(READ_AI_L3_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = createDirectBuffer(READ_BODY_BUFFER_SIZE);

    private ThreadPoolExecutor dataWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor aiL1IndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor aiL2IndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor aiL3IndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    public DefaultMessageStoreImpl() {
        // Create stage file for each thread
        for (int i = 0; i < stageFileList.length; i++) {
            StageFile stageFile = new StageFile(i);
            stageFileList[i] = stageFile;
        }
        Arrays.fill(threadMinT, -1);
    }

    @Override
    public void put(Message message) {
        int threadId = threadIdHolder.get();
        if (tBase < 0) {
            // Set T base as the minimum T of each thread's first message
            threadMinT[threadId] = message.getT();
            int putId = putCounter.getAndIncrement();
            if (putId == 0) {
                long min = Long.MAX_VALUE;
                //noinspection ForLoopReplaceableByForEach
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
        // Write message to each thread's own stage file
        stageFileList[threadId].writeMessage(message);
    }

    private int rewriteFiles() {
        logger.info("Start rewriting files");
        int rewriteCount = 0;
        int currentT = 0;
        Message[] dataBuffer = new Message[TMP_DATA_BUFFER_SIZE];
        long[] aiL1Buffer = new long[TMP_AI_L1_BUFFER_SIZE];
        long[] aiL2Buffer = new long[TMP_AI_L2_BUFFER_SIZE];
        long[] aiL3Buffer = new long[TMP_AI_L3_BUFFER_SIZE];
        int dataIndex = 0;
        int aiL1Index = 0;
        int aiL2Index = 0;
        int aiL3Index = 0;

        // Set T max value as the maximum T of each thread's last message
        tMaxValue = Arrays.stream(stageFileList).mapToLong(StageFile::getLastT).max().orElse(0);
        logger.info("Determined T max value: " + tMaxValue);

        // Use T index & summary to lookup offset for any T in final sorted file
        // NOTE: overflowed length (>= Short.MAX) will be stored in additional map
        tIndex = new short[(int) (tMaxValue - tBase + 1)];
        tIndexSummary = new int[tIndex.length / T_INDEX_SUMMARY_FACTOR + 1];
        logger.info("Created T index and summary with length: " + tIndex.length);

        Arrays.stream(stageFileList).forEach(StageFile::prepareForRead);

        // External file merge sort
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
                    // Persist message to sorted data file asynchronously
                    dataBuffer[dataIndex++] = message;
                    if (dataIndex == dataBuffer.length) {
                        persistToDataFile(dataBuffer, dataIndex);
                        dataIndex = 0;
                    }
                    // Store A to 3-level index file buffer (persist later)
                    aiL1Buffer[aiL1Index++] = message.getA();
                    aiL2Buffer[aiL2Index++] = message.getA();
                    aiL3Buffer[aiL3Index++] = message.getA();

                    rewriteCount++;
                    repeatCount++;
                }
            }
            if (doneCount == stageFileList.length) {
                logger.info("All stage files have done reading");
                break;
            }

            // Update T index
            if (repeatCount < Short.MAX_VALUE) {
                tIndex[currentT] = (short) repeatCount;
            } else {
                tIndex[currentT] = Short.MAX_VALUE;
                tIndexOverflow.put(currentT, repeatCount);
            }
            currentT++;

            // update T index summary
            if (currentT % T_INDEX_SUMMARY_FACTOR == 0) {
                tIndexSummary[currentT / T_INDEX_SUMMARY_FACTOR] = rewriteCount;
            }

            // Persist buffers to 3-level A index file asynchronously
            if (currentT % A_INDEX_LEVEL1_BLOCK_SIZE == 0) {
                persistToAIndexFile(aiL1Buffer, aiL1Index, aiL1IndexWriter, level1IndexFile, false);
                aiL1Index = 0;
            }
            if (currentT % A_INDEX_LEVEL2_BLOCK_SIZE == 0) {
                persistToAIndexFile(aiL2Buffer, aiL2Index, aiL2IndexWriter, level2IndexFile, false);
                aiL2Index = 0;
            }
            if (currentT % A_INDEX_LEVEL3_BLOCK_SIZE == 0) {
                persistToAIndexFile(aiL3Buffer, aiL3Index, aiL3IndexWriter, level3IndexFile, false);
                aiL3Index = 0;
            }
        }

        while (pendingTasks.get() > 0) {
            logger.info("Waiting for all file writers to finish");
            LockSupport.parkNanos(10_000_000);
        }

        // Shut down all file writer threads
        dataWriter.shutdown();
        aiL1IndexWriter.shutdown();
        aiL2IndexWriter.shutdown();
        aiL3IndexWriter.shutdown();
        logger.info("All file writers have finished");

        // Flush remaining buffers
        for (int i = 0; i < dataIndex; i++) {
            Message m = dataBuffer[i];
            dataFile.writeA(m.getA());
            dataFile.writeBody(m.getBody());
        }
        dataFile.flushABuffer();
        dataFile.flushBodyBuffer();
        level1IndexFile.flushABuffer();
        level2IndexFile.flushABuffer();
        level3IndexFile.flushABuffer();

        // Stage files can be discarded now
        stageFileList = null;
        System.gc();
        logger.info("Done rewriting with msg count: " + rewriteCount + ", t overflow: " + tIndexOverflow.size());
        return rewriteCount;
    }

    private void persistToDataFile(Message[] dataBuffer, int size) {
        Message[] persistDataBuffer = new Message[size];
        System.arraycopy(dataBuffer, 0, persistDataBuffer, 0, size);
        pendingTasks.incrementAndGet();
        dataWriter.execute(() -> {
            for (int i = 0; i < size; i++) {
                Message m = persistDataBuffer[i];
                dataFile.writeA(m.getA());
                dataFile.writeBody(m.getBody());
            }
            pendingTasks.decrementAndGet();
        });
    }

    private void persistToAIndexFile(long[] aBuffer, int size, Executor executor, IndexFile indexFile, boolean isAccum) {
        long[] persistBuffer = new long[size];
        pendingTasks.incrementAndGet();
        System.arraycopy(aBuffer, 0, persistBuffer, 0, size);

        executor.execute(() -> {
            Arrays.parallelSort(persistBuffer);
            long[] metaIndex = new long[(size - 1) / A_INDEX_META_FACTOR + 1 + 1];
            long sum = 0;
            for (int i = 0; i < size; i++) {
                long a = persistBuffer[i];
                if (isAccum) {
                    // Accumulated mode: calculate range sum by storing accumulated sum
                    sum += a;
                    indexFile.writeA(sum);
                } else {
                    indexFile.writeA(a);
                }
                if (i % A_INDEX_META_FACTOR == 0) {
                    metaIndex[i / A_INDEX_META_FACTOR] = a;
                }
            }
            // Store max A as last element
            metaIndex[metaIndex.length - 1] = persistBuffer[size - 1];
            indexFile.addMetaIndex(metaIndex);
            if (isAccum) {
                indexFile.addRangeSum(sum);
            }
            pendingTasks.decrementAndGet();
        });
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (!rewriteDone) {
            synchronized (this) {
                if (!rewriteDone) {
                    long totalSize = 0;
                    for (StageFile stageFile : stageFileList) {
                        stageFile.flushBuffer();
                        totalSize += stageFile.fileSize();
                    }
                    logger.info("Flushed all stage files, total size: " + totalSize
                            + ", msg count: " + totalSize / STAGE_MSG_BYTE_LENGTH);

                    int rewriteCount = rewriteFiles();
                    if (rewriteCount != totalSize / STAGE_MSG_BYTE_LENGTH) {
                        logger.info("Inconsistent msg count!");
                        throw new RuntimeException("Abort!");
                    }
                    rewriteDone = true;
                }
            }
        }
        tMin = Math.max(tMin, tBase);
        tMax = Math.min(tMax, tMaxValue);

        ArrayList<Message> result = new ArrayList<>(RESULT_ARRAY_INIT_CAPACITY);
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

        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        int count = 0;
        SumAndCount result;

        // Convert to T diff (T - tBase)
        int tMinDiff = (int) Math.max(tMin - tBase, 0);
        int tMaxDiff = (int) (Math.min(tMax, tMaxValue) - tBase);
        if (tMaxDiff < 0) {
            return 0;
        }

        // Determine start/end index for A index block (using smallest block size)
        int tStart = tMinDiff - tMinDiff % A_INDEX_LEVEL3_BLOCK_SIZE;
        if (tMinDiff != tStart) {
            tStart += A_INDEX_LEVEL3_BLOCK_SIZE;
        }
        int tEnd = (tMaxDiff + 1) / A_INDEX_LEVEL3_BLOCK_SIZE * A_INDEX_LEVEL3_BLOCK_SIZE; // exclusive

        if (tStart >= tEnd) {
            // Process whole query from data file
            result = getAvgValueFromDataFile(aMin, aMax, tMinDiff, tMaxDiff);
            sum += result.getSum();
            count += result.getCount();
        } else {
            // Process head range query from data file
            if (tMinDiff != tStart) {
                result = getAvgValueFromDataFile(aMin, aMax, tMinDiff, tStart - 1);
                sum += result.getSum();
                count += result.getCount();
            }
            // Process tail range query from data file
            if (tMaxDiff > tEnd - 1) {
                result = getAvgValueFromDataFile(aMin, aMax, tEnd, tMaxDiff);
                sum += result.getSum();
                count += result.getCount();
            }
            // Split middle range query into multiple sub queries, which can serve from sorted index file
            int t = tStart;
            while (t < tEnd) {
                // Try to fit into larger block (level1 > level2 > level3)
                if (t % A_INDEX_LEVEL1_BLOCK_SIZE == 0 && t + A_INDEX_LEVEL1_BLOCK_SIZE <= tEnd) {
//                    result = getAvgValueFromAccumIndex(aMin, aMax, t, t + A_INDEX_MAIN_BLOCK_SIZE - 1,
//                            mainIndexFile, A_INDEX_MAIN_BLOCK_SIZE, threadBufferForReadAIM.get());
                    result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_LEVEL1_BLOCK_SIZE - 1,
                            level1IndexFile, A_INDEX_LEVEL1_BLOCK_SIZE, threadBufferForReadAiL1.get());
                    t += A_INDEX_LEVEL1_BLOCK_SIZE;

                } else if (t % A_INDEX_LEVEL2_BLOCK_SIZE == 0 && t + A_INDEX_LEVEL2_BLOCK_SIZE <= tEnd) {
                    result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_LEVEL2_BLOCK_SIZE - 1,
                            level2IndexFile, A_INDEX_LEVEL2_BLOCK_SIZE, threadBufferForReadAiL2.get());
                    t += A_INDEX_LEVEL2_BLOCK_SIZE;

                } else {
                    result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_LEVEL3_BLOCK_SIZE - 1,
                            level3IndexFile, A_INDEX_LEVEL3_BLOCK_SIZE, threadBufferForReadAiL3.get());
                    t += A_INDEX_LEVEL3_BLOCK_SIZE;
                }
                sum += result.getSum();
                count += result.getCount();
            }
        }
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

    private SumAndCount getAvgFromSortedIndex(long aMin, long aMax, int tMin, int tMax,
                                              IndexFile indexFile, int blockSize, ByteBuffer aiByteBufferForRead) {
        long sum = 0;
        int count = 0;
        long[] metaIndex = indexFile.getMetaIndex(tMin / blockSize);
        long rangeMin = metaIndex[0];
        long rangeMax = metaIndex[metaIndex.length - 1];

        if (rangeMin > aMax) {
            return new SumAndCount(0, 0);
        } else if (rangeMax < aMin) {
            return new SumAndCount(0, 0);
        }

        long fullStartOffset = getOffsetByTDiff(tMin);
        long fullEndOffset = getOffsetByTDiff(tMax + 1);

        long startOffset = fullStartOffset;
        long endOffset = fullEndOffset;

        // Binary search for start offset
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

        // Binary search for end offset
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

    private SumAndCount getAvgValueFromAccumIndex(long aMin, long aMax, int tMin, int tMax,
                                                  IndexFile indexFile, int blockSize, ByteBuffer aiByteBufferForRead) {
        long sum;
        int count;
        long[] metaIndex = indexFile.getMetaIndex(tMin / blockSize);
        long rangeMin = metaIndex[0];
        long rangeMax = metaIndex[metaIndex.length - 1];
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
            // Binary search for start offset
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
            // Binary search for end offset
            int end = metaIndex.length - 1;
            while (start < end) {
                int index = (start + end) / 2;
                if (metaIndex[index] <= aMax) {
                    start = index + 1;
                } else {
                    end = index;
                }
            }
            endOffset = fullStartOffset + (end - 1) * A_INDEX_META_FACTOR;
        } else {
            realEndOffset = fullEndOffset;
        }

        long startPrevSum = 0;
        long endSum = rangeSum;

        if (realStartOffset < 0) {
            // Need to find real first offset
            aiByteBufferForRead.flip();
            long prevSum = 0;
            for (long offset = startOffset; offset < fullEndOffset; offset++) {
                if (offset == startOffset + A_INDEX_META_FACTOR) {
                    // Next one should be larger than aMin
                    realStartOffset = offset;
                    startPrevSum = prevSum;
                    break;
                }
                if (aiByteBufferForRead.remaining() == 0) {
                    indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, fullEndOffset);
                }
                long aSum = aiByteBufferForRead.getLong();
                // First one should not be in range (< aMin)
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

        if (realEndOffset < 0) {
            // Need to find real last offset
            aiByteBufferForRead.flip();
            long prevSum = 0;
            for (long offset = endOffset; offset < fullEndOffset; offset++) {
                if (offset == endOffset + A_INDEX_META_FACTOR) {
                    // Next one should be large than aMax
                    realEndOffset = offset;
                    endSum = prevSum;
                    break;
                }
                if (aiByteBufferForRead.remaining() == 0) {
                    indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, fullEndOffset);
                }
                long aSum = aiByteBufferForRead.getLong();
                // First one should not be in range (<= aMax)
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
}
