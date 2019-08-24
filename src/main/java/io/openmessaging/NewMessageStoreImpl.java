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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    private static final int MAX_MEM_TABLE_SIZE = 128 * 1024;
    private static final int PERSIST_BUFFER_SIZE = 4 * 1024 * 1024;

    private static final int DATA_SEGMENT_SIZE = 4 * 1024 * 1024;
//    private static final int DATA_SEGMENT_SIZE = 99 * 1000;

    // TODO split into multiple indexes
    private static final int T_INDEX_SIZE = 1200 * 1024 * 1024;
    private static final int T_INDEX_SUMMARY_FACTOR = 32;

    private static final int WRITE_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024 * 8;

    private static final int WRITE_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;
    private static final int READ_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;

    private static final int PERSIST_SAMPLE_RATE = 100;
    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    private static List<DataFile> dataFileList = new ArrayList<>();
    private static DataFile curDataFile = null;

    private static ByteBuffer aBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);
    private static ByteBuffer bodyBufferForWrite = ByteBuffer.allocateDirect(WRITE_BODY_BUFFER_SIZE);

    static {
        logger.info("LsmMessageStoreImpl load start");
//        logger.info("LsmMessageStoreImpl load end");
    }

    private ThreadPoolExecutor persistThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);
    private int persistCounter = 0;
    private int dataFileCounter = 0;

    private volatile Collection<Message> memTable = createMemTable();
//    private Collection<Message> memTable = createMemTable();

    private Message[] persistBuffer1 = new Message[PERSIST_BUFFER_SIZE];
    private Message[] persistBuffer2 = new Message[PERSIST_BUFFER_SIZE];
    private int persistBufferIndex = 0;
    private volatile boolean persistDone = false;

    private short[] tIndex = new short[T_INDEX_SIZE];
    private int[] tIndexSummary = new int[T_INDEX_SIZE / T_INDEX_SUMMARY_FACTOR];
    private long[] tCurrent = new long[PRODUCER_THREAD_NUM];
    private int tIndexCounter = 0;
    private long tBase = -1;

    private long maxBufferIndex = Long.MIN_VALUE;

    private Message sentinelMessage = new Message(T_UPPER_LIMIT, A_UPPER_LIMIT, null);

    private ThreadLocal<ByteBuffer> threadBufferForReadA = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForReadBody = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BODY_BUFFER_SIZE));

    private AtomicInteger threadIdCounter = new AtomicInteger(0);
    private ThreadLocal<Integer> threadId = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());

    @Override
    public void put(Message message) {
        long putStart = System.nanoTime();
        int putId = putCounter.getAndIncrement();
        if (IS_TEST_RUN && putId == 0) {
            _putStart = System.currentTimeMillis();
        }
        if (IS_TEST_RUN && putId == 10000 * 10000) {
            throw new RuntimeException("" + (System.currentTimeMillis() - _putStart));
        }
//        if (putId % PUT_SAMPLE_RATE == 0) {
//            logger.info("Before add, time: " + (System.nanoTime() - putStart));
//        }

        synchronized (this) {
            memTable.add(message);
        }

        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("Put message to memTable with t: " + message.getT() + ", a: " + message.getA()
                    + ", time: " + (System.nanoTime() - putStart) + ", putId: " + putId);
        }

        tCurrent[threadId.get()] = message.getT();

        if ((putId + 1) % MAX_MEM_TABLE_SIZE == 0) {
//            logger.info("Submit memTable persist task, putId: " + putId);
            long currentMinT = tCurrent[0];
            for (int i = 1; i < tCurrent.length; i++) {
                currentMinT = Math.min(currentMinT, tCurrent[i]);
            }
            long finalCurrentMinT = currentMinT;

            Collection<Message> frozenMemTable;
            synchronized (this) {
                frozenMemTable = memTable;
                memTable = createMemTable();
            }

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
//        return new ConcurrentSkipListSet<>((m1, m2) -> {
        return new TreeSet<>((m1, m2) -> {
            if (m1.getT() == m2.getT()) {
                //noinspection ComparatorMethodParameterNotUsed
                return -1;
            }
            // Order by T desc
            return m2.getT() < m1.getT() ? -1 : 1;
        });
    }

    private void persistMemTable(Collection<Message> frozenMemTable, long currentMinT) {
        long persistStart = System.currentTimeMillis();
        int persistId = persistCounter++;
        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable with size: " + frozenMemTable.size()
                    + ", buffer index: " + persistBufferIndex + ", persistId: " + persistId);
        }

        Message[] sourceBuffer = persistId % 2 == 0? persistBuffer1 : persistBuffer2;
        Message[] targetBuffer = persistId % 2 == 1? persistBuffer1 : persistBuffer2;

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
        maxBufferIndex = Math.max(maxBufferIndex, persistBufferIndex);

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
                int tDiff = (int) (lastT - tBase);

                // update t index
                if (msgCount < Short.MAX_VALUE) {
                    tIndex[tDiff] = (short) msgCount;
                } else {
                    // TODO store overflowed count to additional map
                    throw new RuntimeException("A count is larger than short max");
                }

//                // sort by a
//                if (msgCount > 1) {
//                    msgBuffer.sort(Comparator.comparingLong(Message::getA));
//                    sortCount += msgCount;
//                    sortTimes++;
//                }

                // store a and body
                for (Message message : msgBuffer) {
                    if (tIndexCounter % DATA_SEGMENT_SIZE == 0) {
                        if (curDataFile != null) {
                            curDataFile.flushABuffer();
                            curDataFile.flushBodyBuffer();
//                            logger.info("Flushed data file " + curDataFile.index + " at offset: " + tIndexCounter);
                        }
                        curDataFile = newDataFile(tIndexCounter, tIndexCounter + DATA_SEGMENT_SIZE - 1);
                    }
                    curDataFile.writeA(message.getA());
                    curDataFile.writeBody(message.getBody());
                    tIndexCounter++;
                }
                msgBuffer.clear();

                // update t index summary
                if (t < T_UPPER_LIMIT) {
                    tDiff = (int) (t - tBase);
                    if (tDiff % T_INDEX_SUMMARY_FACTOR == 0) {
                        tIndexSummary[(tDiff / T_INDEX_SUMMARY_FACTOR)] = tIndexCounter;
                    }
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
        if (getId == 0) {
            logger.info("Waiting for previous persist tasks");
            while (persistThreadPool.getActiveCount() + persistThreadPool.getQueue().size() > 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Flush last mem table");
            persistMemTable(memTable, T_UPPER_LIMIT);

            curDataFile.flushABuffer();
            curDataFile.flushBodyBuffer();
//            logger.info("Flushed data file with start " + curDataFile.start + " at end");

//            verifyData();
            persistDone = true;
            persistThreadPool.shutdown();
            persistBuffer1 = null;
            persistBuffer2 = null;
            System.gc();

            logger.info("Flushed all memTables, msg count1: " + putCounter.get()
                    + ", msg count2: " + tIndexCounter
                    + ", persist buffer index: " + persistBufferIndex
                    + ", max buffer index: " + maxBufferIndex
                    + ", time: " + (System.currentTimeMillis() - getStart));
//            if (putCounter.get() != tIndexCounter) {
//                throw new RuntimeException("Inc: " + (putCounter.get() - tIndexCounter));
//            }
        }
        while (!persistDone) {
//            logger.info("Waiting for all persist tasks to finish");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (persistDone) {
                logger.info("All persist tasks has finished, time: " + (System.currentTimeMillis() - getStart));
            }
        }

        ArrayList<Message> result = new ArrayList<>(4096);
        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
        aByteBufferForRead.flip();
        bodyByteBufferForRead.flip();

        int tDiff = (int) (tMin - tBase);
        long offset = getOffsetByTDiff(tDiff);

        for (long t = tMin; t <= tMax; t++) {
            int msgCount = tIndex[tDiff++];
            while (msgCount > 0) {
                if (!aByteBufferForRead.hasRemaining()) {
                    fillReadABuffer(aByteBufferForRead, offset);
                }
                long a = aByteBufferForRead.getLong();
                if (a >= aMin && a <= aMax) {
                    if (!bodyByteBufferForRead.hasRemaining()) {
                        fillReadBodyBuffer(bodyByteBufferForRead, offset);
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

        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        aByteBufferForRead.flip();

        int tDiff = (int) (tMin - tBase);
        long offset = getOffsetByTDiff(tDiff);

        for (long t = tMin; t <= tMax; t++) {
            int msgCount = tIndex[tDiff++];
            while (msgCount > 0) {
                if (aByteBufferForRead.remaining() == 0) {
                    fillReadABuffer(aByteBufferForRead, offset);
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

        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("Got " + count
                    + ", time: " + (System.currentTimeMillis() - avgStart));
        }
        if (IS_TEST_RUN) {
            avgMsgCounter.addAndGet(count);
        }
        return count == 0 ? 0 : sum / count;
    }

    private void verifyData() {
        int totalCount = 0;
        for (long t = tBase; t < tBase + tIndex.length; t++) {
            assert t % T_INDEX_SUMMARY_FACTOR != 0
                    || tIndexSummary[(int) (t / T_INDEX_SUMMARY_FACTOR)] == totalCount;
            int tDiff = (int) (t - tBase);
            int count = tIndex[tDiff];
            totalCount += count;
            if (t % PUT_SAMPLE_RATE == 0) {
                logger.info("Total count: " + totalCount + ", t: " + t);
            }
        }
        logger.info("Total count: " + totalCount);

//        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
//        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
//        aByteBufferForRead.flip();
//        bodyByteBufferForRead.flip();
//        long offset = 0;
//
//        while (true) {
//            if (!aByteBufferForRead.hasRemaining()) {
//                try {
//                    aByteBufferForRead.clear();
//                    int nBytes = aFileChannel.read(aByteBufferForRead, offset);
//                    if (nBytes <= 0) {
//                        break;
//                    }
//                    offset += nBytes;
//                    aByteBufferForRead.flip();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
////            long a = aByteBufferForRead.getLong();
////            logger.info(a);
//        }
//        logger.info("Offset for A: " + offset);
//
//        aByteBufferForRead.clear();
//        bodyByteBufferForRead.clear();
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
                    Constants.DATA_DIR + "body" + dataFile.index + ".data", "rw");
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

    private static class DataFile {
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
                logger.info("[ERROR] Write to channel failed: " + e.getMessage());
            }
            aBufferForWrite.clear();
        }

        void flushBodyBuffer() {
            bodyBufferForWrite.flip();
            try {
                bodyChannel.write(bodyBufferForWrite);
            } catch (IOException e) {
                logger.info("[ERROR] Write to channel failed: " + e.getMessage());
            }
            bodyBufferForWrite.clear();
        }
    }

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);

    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;
}
