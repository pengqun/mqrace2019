package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.Constants.*;

/**
 * @author .ignore 2019-07-29
 */
public class LsmMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(LsmMessageStoreImpl.class);

    private static final int MAX_MEM_TABLE_SIZE = 1 * 1024;
    private static final int PERSIST_BUFFER_SIZE = 1024 * 1024;

    private static final int T_INDEX_SIZE = 1024 * 1024 * 1024;
    private static final int T_INDEX_SUMMARY_FACTOR = 32;

    private static final int WRITE_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;

    private static final int WRITE_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;
    private static final int READ_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;

    private static final int PERSIST_SAMPLE_RATE = 10000;
    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    private static FileChannel aFileChannel;
    private static FileChannel bodyFileChannel;
    private static ByteBuffer aByteBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);
    private static ByteBuffer bodyByteBufferForWrite = ByteBuffer.allocateDirect(WRITE_BODY_BUFFER_SIZE);

    static {
        logger.info("LsmMessageStoreImpl loaded");
        try {
            RandomAccessFile aFile = new RandomAccessFile(Constants.DATA_DIR + "a.data", "rw");
            aFileChannel = aFile.getChannel();
            RandomAccessFile bodyFile = new RandomAccessFile(Constants.DATA_DIR + "body.data", "rw");
            bodyFileChannel = bodyFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private ThreadPoolExecutor persistThreadPool = new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger persistCounter = new AtomicInteger(0);
    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);

    private volatile Collection<Message> memTable = createMemTable();
    private Message[] persistBuffer1 = new Message[PERSIST_BUFFER_SIZE];
    private Message[] persistBuffer2 = new Message[PERSIST_BUFFER_SIZE];
    private int persistBufferIndex = 0;
    private volatile boolean persistDone = false;

    private short[] tIndex = new short[T_INDEX_SIZE];
    private int[] tIndexSummary = new int[T_INDEX_SIZE / T_INDEX_SUMMARY_FACTOR];
    private long[] tCurrent = new long[PRODUCER_THREAD_NUM];
    private int tIndexCounter = 0;
    private long tBase = -1;

    private Message sentinelMessage = new Message(Long.MAX_VALUE, Long.MAX_VALUE, null);

    private ThreadLocal<ByteBuffer> threadBufferForReadA = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BODY_BUFFER_SIZE));

    private AtomicInteger threadIdCounter = new AtomicInteger(0);
    private ThreadLocal<Integer> threadId = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());

    @Override
    public void put(Message message) {
//        long putStart = System.nanoTime();
        int putId = putCounter.getAndIncrement();
        if (IS_TEST_RUN && putId == 0) {
            _putStart = System.currentTimeMillis();
            _firstStart = _putStart;
        }
        if (IS_TEST_RUN && putId == 10000 * 10000) {
            throw new RuntimeException("" + (System.currentTimeMillis() - _putStart));
        }
//        if (putId % PUT_SAMPLE_RATE == 0) {
//            logger.info("Before add, time: " + (System.nanoTime() - putStart));
//        }

        memTable.add(message);

//        if (putId % PUT_SAMPLE_RATE == 0) {
//            logger.info("Put message to memTable with t: " + message.getT() + ", a: " + message.getA()
//                    + ", time: " + (System.nanoTime() - putStart) + ", putId: " + putId);
//        }

        tCurrent[threadId.get()] = message.getT();

        if ((putId + 1) % MAX_MEM_TABLE_SIZE == 0) {
//            logger.info("Submit memTable persist task, putId: " + putId);
            long currentMinT = tCurrent[0];
            for (int i = 1; i < tCurrent.length; i++) {
                currentMinT = Math.min(currentMinT, tCurrent[i]);
            }
            long finalCurrentMinT = currentMinT;

            Collection<Message> frozenMemTable = memTable;
            memTable = createMemTable();

            persistThreadPool.execute(() -> {
                try {
                    persistMemTable(frozenMemTable, finalCurrentMinT);
                } catch (Exception e) {
                    logger.info("Failed to persist mem table", e);
                    System.exit(-1);
                }
            });
//            logger.info("Submitted memTable persist task, time: "
//                    + (System.currentTimeMillis() - putStart) + ", putId: " + putId);
        }
//        if (putId % PUT_SAMPLE_RATE == 0) {
//            logger.info("Done put, time: " + (System.nanoTime() - putStart));
//        }
    }

    private Collection<Message> createMemTable() {
        return new ConcurrentSkipListSet<>((m1, m2) -> {
//        return new TreeSet<>((m1, m2) -> {
            if (m1.getT() == m2.getT()) {
                //noinspection ComparatorMethodParameterNotUsed
                return -1;
            }
            // Order by T desc
            return (int) (m2.getT() - m1.getT());
        });
    }

    private void persistMemTable(Collection<Message> frozenMemTable, long currentMinT) {
        long persistStart = System.currentTimeMillis();
        int persistId = persistCounter.getAndIncrement();
        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable with size: " + frozenMemTable.size()
                    + ", buffer index: " + persistBufferIndex + ", persistId: " + persistId);
        }

        Message[] sourceBuffer = persistId % 2 == 0? persistBuffer1 : persistBuffer2;
        Message[] targetBuffer = persistId % 2 == 1? persistBuffer1 : persistBuffer2;

//        try {
//            Thread.sleep(1);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        int i = 0;
        int j = 0;
        for (Message message : frozenMemTable) {
            while (i < persistBufferIndex && sourceBuffer[i].getT() >= message.getT()) {
                targetBuffer[j++] = sourceBuffer[i++];
            }
            targetBuffer[j++] = message;
        }
        while (i < persistBufferIndex) {
            targetBuffer[j++] = sourceBuffer[i++];
        }
        persistBufferIndex = j;
        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Copied memTable with size: " + frozenMemTable.size() + " to buffer with index: " + persistBufferIndex
                    + ", time: " + (System.currentTimeMillis() - persistStart) + ", persistId: " + persistId);
        }

        if (tBase == -1) {
            tBase = targetBuffer[persistBufferIndex - 1].getT();
            logger.info("Determined T base: " + tBase);
        }

        if (persistBufferIndex == 0) {
            logger.info("[WARN] persistBufferIndex is 0, skipped");
            return;
        }

        int index = persistBufferIndex - 1;
        long lastT = targetBuffer[index].getT();
        List<Message> msgBuffer = new ArrayList<>();

        while (true) {
            Message msg = index >= 0 ? targetBuffer[index] : sentinelMessage;
            long t = msg.getT();

            if (t != lastT) {
                int msgCount = msgBuffer.size();

                // update t index
                if (msgCount < Short.MAX_VALUE) {
                    tIndex[(int) (lastT - tBase)] = (short) msgCount;
                } else {
                    // TODO store overflowed count to additional map
                    throw new RuntimeException("A count is larger than short max");
                }

                // sort by a
                if (msgCount > 1) {
                    msgBuffer.sort((m1, m2) -> (int) (m1.getA() - m2.getA()));
                }

                // store a and body
                for (Message message : msgBuffer) {
                    if (!aByteBufferForWrite.hasRemaining()) {
                        flushBuffer(aFileChannel, aByteBufferForWrite);
                    }
                    aByteBufferForWrite.putLong(message.getA());
                    if (!bodyByteBufferForWrite.hasRemaining()) {
                        flushBuffer(bodyFileChannel, bodyByteBufferForWrite);
                    }
                    bodyByteBufferForWrite.put(message.getBody());
                }
                tIndexCounter += msgCount;
                msgBuffer.clear();

                // update t index summary
                if ((t - tBase) % T_INDEX_SUMMARY_FACTOR == 0) {
                    tIndexSummary[(int) ((t - tBase) / T_INDEX_SUMMARY_FACTOR)] = tIndexCounter;
                }
            }

            if (t >= currentMinT) {
                break;
            }

            msgBuffer.add(msg);
            lastT = t;
            index--;
        }
        persistBufferIndex = index + 1;

        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Done persisting memTable with size: " + frozenMemTable.size()
                    + ", buffer index: " + persistBufferIndex
                    + ", time: " + (System.currentTimeMillis() - persistStart) + ", persistId: " + persistId);
        }
    }

    private void flushBuffer(FileChannel fileChannel, ByteBuffer byteBuffer) {
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer);
        } catch (IOException e) {
            logger.info("[ERROR] Write to channel failed: " + e.getMessage());
        }
        byteBuffer.clear();
    }

    private long getOffsetByT(long t) {
        int tDiff = (int) (t - tBase);
        long offset = tIndexSummary[tDiff / T_INDEX_SUMMARY_FACTOR];
        for (int i = tDiff / T_INDEX_SUMMARY_FACTOR * T_INDEX_SUMMARY_FACTOR; i < t; i++) {
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
        if (getId == 0) {
            logger.info("Flush all memTables before getMessage");
            while (persistThreadPool.getActiveCount() + persistThreadPool.getQueue().size() > 0) {
                logger.info("Waiting for previous persist tasks to finish");
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            persistMemTable(memTable, Long.MAX_VALUE);
            flushBuffer(aFileChannel, aByteBufferForWrite);
            flushBuffer(bodyFileChannel, bodyByteBufferForWrite);
//            verifyData();
            persistDone = true;

            persistThreadPool.shutdown();
            logger.info("Flushed all memTables, time: " + (System.currentTimeMillis() - getStart));
        }
        while (!persistDone) {
            logger.info("Waiting for all persist tasks to finish");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (persistDone) {
                logger.info("All persist tasks has finished, time: " + (System.currentTimeMillis() - getStart));
            }
        }

        ArrayList<Message> result = new ArrayList<>(1024);

        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
        aByteBufferForRead.flip();
        bodyByteBufferForRead.flip();

        long offset = getOffsetByT(tMin);
        int tDiff = (int) (tMin - tBase);

        for (long t = tMin; t <= tMax; t++) {
            int msgCount = tIndex[tDiff++];
            while (msgCount-- > 0) {
                if (!aByteBufferForRead.hasRemaining()) {
                    try {
                        aByteBufferForRead.clear();
                        aFileChannel.read(aByteBufferForRead, offset * KEY_A_BYTE_LENGTH);
                        aByteBufferForRead.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (!bodyByteBufferForRead.hasRemaining()) {
                    try {
                        bodyByteBufferForRead.clear();
                        bodyFileChannel.read(bodyByteBufferForRead, offset * BODY_BYTE_LENGTH);
                        bodyByteBufferForRead.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    byte[] body = new byte[BODY_BYTE_LENGTH];
                    bodyByteBufferForRead.get(body);
                    Message msg = new Message(a, t, body);
                    result.add(msg);
                } else {
                    bodyByteBufferForRead.position(bodyByteBufferForRead.position() + BODY_BYTE_LENGTH);
                }
                offset++;
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
        long skip = 0;

        long offset = getOffsetByT(tMin);

        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        aByteBufferForRead.flip();

        for (long t = tMin; t <= tMax; t++) {
            int aCount = tIndex[(int) (t - tBase)];
            while (aCount-- > 0) {
                if (aByteBufferForRead.remaining() == 0) {
                    try {
                        aByteBufferForRead.clear();
                        aFileChannel.read(aByteBufferForRead, offset * KEY_A_BYTE_LENGTH);
                        aByteBufferForRead.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                } else {
                    skip++;
                }
                offset++;
            }
        }
        aByteBufferForRead.clear();

        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("Got " + count + ", skip: " + skip
                    + ", time: " + (System.currentTimeMillis() - avgStart));
        }
        if (IS_TEST_RUN) {
            avgMsgCounter.addAndGet((int) count);
        }
        return count == 0 ? 0 : sum / count;
    }

    private void verifyData() {
        int totalCount = 0;
        for (long t = tBase; t < 1000000; t++) {
            if (t % T_INDEX_SUMMARY_FACTOR == 0) {
                assert tIndexSummary[(int) (t / T_INDEX_SUMMARY_FACTOR)] == totalCount;
            }
            int tDiff = (int) (t - tBase);
            int count = tIndex[tDiff];
            totalCount += count;
        }
        logger.info("Total count: " + totalCount);

        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
        aByteBufferForRead.flip();
        bodyByteBufferForRead.flip();
        long offset = 0;

        while (true) {
            if (!aByteBufferForRead.hasRemaining()) {
                try {
                    aByteBufferForRead.clear();
                    int nBytes = aFileChannel.read(aByteBufferForRead, offset);
                    if (nBytes <= 0) {
                        break;
                    }
                    offset += nBytes;
                    aByteBufferForRead.flip();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            long a = aByteBufferForRead.getLong();
//            logger.info(a);
        }
        logger.info("Offset for A: " + offset);

        aByteBufferForRead.clear();
        bodyByteBufferForRead.clear();
    }

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);

    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;
    private long _firstStart = 0;
}
