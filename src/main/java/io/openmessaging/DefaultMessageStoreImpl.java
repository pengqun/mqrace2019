package io.openmessaging;


import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

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
    private AtomicInteger pendingTasks = new AtomicInteger();

    private StageFile[] stageFileList = new StageFile[PRODUCER_THREAD_NUM];
    private DataFile dataFile = new DataFile();
    private IndexFile mainIndexFile = new IndexFile();
    private IndexFile subIndexFile = new IndexFile();

    private ThreadLocal<ByteBuffer> threadBufferForReadA1 = createDirectBuffer(READ_A1_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadA2 = createDirectBuffer(READ_A2_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAIM = createDirectBuffer(READ_AIM_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadAIS = createDirectBuffer(READ_AIS_BUFFER_SIZE);
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = createDirectBuffer(READ_BODY_BUFFER_SIZE);

    private ThreadPoolExecutor dataWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor aimIndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor aisIndexWriter = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    private ForkJoinPool forkJoinPool = new ForkJoinPool(32);

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
        Message[] dataBuffer = new Message[8 * 1024];
        long[] aimBuffer = new long[A_INDEX_MAIN_BLOCK_SIZE * 2048];
        long[] aisBuffer = new long[A_INDEX_SUB_BLOCK_SIZE * 2048];
        int dataIndex = 0;
        int aimIndex = 0;
        int aisIndex = 0;

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

                    dataBuffer[dataIndex++] = message;

                    if (dataIndex == dataBuffer.length) {
                        persistToDataFile(dataBuffer, dataIndex);
                        dataIndex = 0;
                    }

                    aimBuffer[aimIndex++] = message.getA();
                    aisBuffer[aisIndex++] = message.getA();

                    if (rewriteCount % REWRITE_SAMPLE_RATE == 0) {
                        logger.info("Write message to data file: " + rewriteCount);
                    }
//                    if (rewriteCount == 200_000_000) {
//                        while (pendingTasks.get() > 0) {
//                            LockSupport.parkNanos(10_000_000);
//                        }
//                        throw new RuntimeException("" + (System.currentTimeMillis() - rewriteStart) + ", " + rewriteCount);
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

            // store a into multiple index
            if (currentT % A_INDEX_MAIN_BLOCK_SIZE == 0) {
                persistToAIndexFile(aimBuffer, aimIndex, aimIndexWriter, mainIndexFile, USE_ACCUMULATED_SUM);
//                persistToAIndexFile(aimBuffer, aimIndex, aimIndexWriter, mainIndexFile, false);
                aimIndex = 0;
            }
            if (currentT % A_INDEX_SUB_BLOCK_SIZE == 0) {
                persistToAIndexFile(aisBuffer, aisIndex, aisIndexWriter, subIndexFile, USE_ACCUMULATED_SUM);
//                persistToAIndexFile(aisBuffer, aisIndex, aisIndexWriter, subIndexFile, false);
                aisIndex = 0;
            }
        }

        while (pendingTasks.get() > 0) {
            logger.info("Waiting for a file writer to finish");
            LockSupport.parkNanos(10_000_000);
        }
        for (int i = 0; i < dataIndex; i++) {
            Message m = dataBuffer[i];
            dataFile.writeA(m.getA());
            dataFile.writeBody(m.getBody());
        }
        dataWriter.shutdown();
        aimIndexWriter.shutdown();
        aisIndexWriter.shutdown();
        logger.info("All file writer has finished");

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
            ByteBuffer byteBuffer = null;
            if (isAccum) {
                byteBuffer = ByteBuffer.allocateDirect(metaIndex.length * A_INDEX_CACHE_LENGTH * KEY_A_BYTE_LENGTH);
            }
            long sum = 0;
            for (int i = 0; i < size; i++) {
                long a = persistBuffer[i];
                if (isAccum) {
                    sum += a;
                    indexFile.writeA(sum);
                } else {
                    indexFile.writeA(a);
                }
                if (i % A_INDEX_META_FACTOR == 0) {
                    metaIndex[i / A_INDEX_META_FACTOR] = a;
                }
                if (isAccum && i % A_INDEX_META_FACTOR < A_INDEX_CACHE_LENGTH) {
                    byteBuffer.putLong(sum);
                }
            }
            metaIndex[metaIndex.length - 1] = persistBuffer[size - 1];
            indexFile.addMetaIndex(metaIndex);
            if (isAccum) {
                indexFile.addRangeSum(sum);
                indexFile.addCacheBuffer(byteBuffer);
            }
            pendingTasks.decrementAndGet();
        });
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

        tMin = Math.max(tMin, tBase);
        tMax = Math.min(tMax, tMaxValue);

        ArrayList<Message> result = new ArrayList<>(1024);
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

    public class GetAvgValueTask extends RecursiveTask<SumAndCount> {
        long aMin;
        long aMax;
        int tMin;
        int tMax;
        int type;
        boolean isMain;

        GetAvgValueTask(long aMin, long aMax, int tMin, int tMax, int type, boolean isMain) {
            this.aMin = aMin;
            this.aMax = aMax;
            this.tMin = tMin;
            this.tMax = tMax;
            this.type = type;
            this.isMain = isMain;
        }

        @Override
        protected SumAndCount compute() {
            if (type == 0) { // normal
                return getAvgValueFromDataFile(aMin, aMax, tMin, tMax);
            }
            if (USE_ACCUMULATED_SUM) {
                return getAvgValueFromAccumIndex(aMin, aMax, tMin, tMax, isMain);
            }
            return getAvgFromSortedIndex(aMin, aMax, tMin, tMax, isMain);
        }
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        long avgStart = System.currentTimeMillis();
        int avgId = avgCounter.getAndIncrement();
        if (avgId == 0) {
            PerfStats._getEnd = System.currentTimeMillis();
            PerfStats._avgStart = PerfStats._getEnd;
        }
//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("getAvgValue - tMin: " + tMin + ", tMax: " + tMax + ", aMin: " + aMin + ", aMax: " + aMax
//                    + ", tRange: " + (tMax - tMin) + ", avgId: " + avgId);
//        }
        if (avgId == TEST_BOUNDARY) {
            PerfStats.printStats(this);
        }

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
            if (USE_FORK_JOIN_POOL) {
                Collection<GetAvgValueTask> tasks = new ArrayList<>();
                // Process head
                if (tMinDiff != tStart) {
                    tasks.add(new GetAvgValueTask(aMin, aMax, tMinDiff, tStart - 1, 0, false));
                }
                // Process tail
                if (tMaxDiff > tEnd - 1) {
                    tasks.add(new GetAvgValueTask(aMin, aMax, tEnd, tMaxDiff, 0, false));
                }
                // Process middle
                int t = tStart;
                while (t < tEnd) {
                    if (t % A_INDEX_MAIN_BLOCK_SIZE == 0 && t + A_INDEX_MAIN_BLOCK_SIZE <= tEnd) {
                        tasks.add(new GetAvgValueTask(aMin, aMax, t, t + A_INDEX_MAIN_BLOCK_SIZE - 1, 1, true));
                        t += A_INDEX_MAIN_BLOCK_SIZE;
                    } else {
                        tasks.add(new GetAvgValueTask(aMin, aMax, t, t + A_INDEX_SUB_BLOCK_SIZE - 1, 1, false));
                        t += A_INDEX_SUB_BLOCK_SIZE;
                    }
                }
                List<SumAndCount> taskResult = tasks.stream().map(forkJoinPool::submit)
                        .map(ForkJoinTask::join)
                        .collect(Collectors.toList());
                for (SumAndCount sumAndCount : taskResult) {
                    sum += sumAndCount.getSum();
                    count += sumAndCount.getCount();
                }
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
                        if (USE_ACCUMULATED_SUM) {
                            result = getAvgValueFromAccumIndex(aMin, aMax, t, t + A_INDEX_MAIN_BLOCK_SIZE - 1, true);
                        } else {
                            result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_MAIN_BLOCK_SIZE - 1, true);
                        }
                        t += A_INDEX_MAIN_BLOCK_SIZE;
                    } else {
                        if (USE_ACCUMULATED_SUM) {
                            result = getAvgValueFromAccumIndex(aMin, aMax, t, t + A_INDEX_SUB_BLOCK_SIZE - 1, false);
                        } else {
                            result = getAvgFromSortedIndex(aMin, aMax, t, t + A_INDEX_SUB_BLOCK_SIZE - 1, false);
                        }
                        t += A_INDEX_SUB_BLOCK_SIZE;
                    }
                sum += result.getSum();
                count += result.getCount();
                }
            }
        }

//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("Got " + count + ", time: " + (System.currentTimeMillis() - avgStart) + ", avgId: " + avgId);
//        }
        avgMsgCounter.addAndGet(count);

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

        long startPos = 0;
        long endPos = 0;

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
            startPos = start;
        } else {
            realStartOffset = fullStartOffset;
        }

        if (aMax < rangeMax) {
            // binary search for end offset (> aMax)
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
            endPos = end - 1;
        } else {
            realEndOffset = fullEndOffset;
        }

        long startPrevSum = 0;
        long endSum = rangeSum;

        ByteBuffer cacheBuffer = indexFile.getCacheBuffer(tMin / blockSize);

        // need to find real first offset
        if (realStartOffset < 0) {
            aiByteBufferForRead.flip();
            long prevSum = 0;
//            boolean diskRead = false;
            for (long offset = startOffset; offset < fullEndOffset; offset++) {
                long aSum;
                if (offset - startOffset < A_INDEX_CACHE_LENGTH) {
                    aSum = cacheBuffer.getLong((int) ((startPos * A_INDEX_CACHE_LENGTH + offset - startOffset) * KEY_A_BYTE_LENGTH));
                } else {
                    if (offset == startOffset + A_INDEX_META_FACTOR) {
                        realStartOffset = offset;
                        startPrevSum = prevSum;
                        break;
                    }
                    if (aiByteBufferForRead.remaining() == 0) {
                        indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, fullEndOffset);
//                        diskRead = true;
                    }
                    aSum = aiByteBufferForRead.getLong();
                }
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
//            if (diskRead) {
//                cacheMiss.incrementAndGet();
//            } else {
//                cacheHit.incrementAndGet();
//            }
            aiByteBufferForRead.clear();
        }

        // need to find real last offset
        if (realEndOffset < 0) {
            aiByteBufferForRead.flip();
            long prevSum = 0;
//            boolean diskRead = false;
            for (long offset = endOffset; offset < fullEndOffset; offset++) {
                long aSum;
                if (offset - endOffset < A_INDEX_CACHE_LENGTH) {
                    aSum = cacheBuffer.getLong((int) ((endPos * A_INDEX_CACHE_LENGTH + offset - endOffset) * KEY_A_BYTE_LENGTH));
                } else {
                    if (offset == endOffset + A_INDEX_META_FACTOR) {
                        realEndOffset = offset;
                        endSum = prevSum;
                        break;
                    }
                    if (aiByteBufferForRead.remaining() == 0) {
                        indexFile.fillReadAIBuffer(aiByteBufferForRead, offset, fullEndOffset);
//                        diskRead = true;
                    }
                    aSum = aiByteBufferForRead.getLong();
                }
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
//            if (diskRead) {
//                cacheMiss.incrementAndGet();
//            } else {
//                cacheHit.incrementAndGet();
//            }
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
