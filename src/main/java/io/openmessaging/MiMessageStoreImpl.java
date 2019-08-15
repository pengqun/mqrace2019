package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.Constants.*;

/**
 * @author .ignore 2019-07-29
 */
@SuppressWarnings("DuplicatedCode")
public class MiMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(MiMessageStoreImpl.class);

    private static final int MAX_MEM_TABLE_SIZE = 10 * 10000;

    private static final int T_INDEX_SIZE = 1000 * 1024 * 1024;
    private static final int T_INDEX_SUMMARY_RATE = 32;
    private static final int T_WRITE_ARRAY_SIZE = 400 * 10000;

    private static final int A_DIFF_BASE_OFFSET = 10000;
    private static final int A_DIFF_HALF_SIZE = 1000 * 1024 * 1024;
//    private static final int A_DIFF_HALF_SIZE = 1000000;

    private static final int WRITE_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;
    private static final int READ_BODY_BUFFER_SIZE = Constants.BODY_BYTE_LENGTH * 1024;

    private static final int PERSIST_SAMPLE_RATE = 100;
    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    private static FileChannel bodyFileChannel;
    private static ByteBuffer bodyByteBufferForWrite = ByteBuffer.allocateDirect(WRITE_BODY_BUFFER_SIZE);

    static {
        logger.info("MiMessageStoreImpl loaded");
        try {
            RandomAccessFile bodyFile = new RandomAccessFile(Constants.DATA_DIR + "body.data", "rw");
            bodyFileChannel = bodyFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private volatile NavigableMap<Long, Message> memTable = new TreeMap<>((Collections.reverseOrder()));

    private ThreadPoolExecutor persistThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger persistCounter = new AtomicInteger(0);
    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);

    private volatile boolean persistDone = false;

    private Message[] msgBuffer1 = new Message[T_WRITE_ARRAY_SIZE];
    private Message[] msgBuffer2 = new Message[T_WRITE_ARRAY_SIZE];
    private int bufferIndex = 0;

    private byte[] tIndex = new byte[T_INDEX_SIZE];
    private int[] tSummary = new int[T_INDEX_SIZE / T_INDEX_SUMMARY_RATE];
    private int[] currentT = new int[PRODUCE_THREAD_NUM];
    private int msgCounter = 0;

    private short[] aFirstHalf = new short[A_DIFF_HALF_SIZE];
    private ByteBuffer aLastHalf = ByteBuffer.allocateDirect(A_DIFF_HALF_SIZE * KEY_A_BYTE_LENGTH);

    private byte[] tIndexDictCount2Id = new byte[MAX_T_INDEX_SIZE];
    private int[] tIndexDictId2Count = new int[128];
    private byte tIndexDictId = 1;
    private short[] aBuffer = new short[MAX_T_INDEX_SIZE];

    private Message sentinelMessage = new Message(Integer.MAX_VALUE, Integer.MAX_VALUE, null);

    private ThreadLocal<ByteBuffer> threadBufferForReadBody = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BODY_BUFFER_SIZE));

    private AtomicInteger threadIdCounter = new AtomicInteger(0);
    private ThreadLocal<Integer> threadId = ThreadLocal.withInitial(() -> threadIdCounter.getAndIncrement());

    @Override
    public void put(Message message) {
        long putStart = System.currentTimeMillis();
        int putId = putCounter.getAndIncrement();
        if (IS_TEST_RUN && putId == 0) {
            _putStart = putStart;
            _firstStart = putStart;
        }
//        if (IS_TEST_RUN && _firstStart > 0 && (putStart - _firstStart) > 60 * 1000) {
//            throw new RuntimeException(":)" + putId + "!");
//        }
        long key = (message.getT() << 32) + putId;
        synchronized (this) {
            memTable.put(key, message);
        }
        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("putMessage to memTable with t: " + message.getT() + ", a: " + message.getA()
                    + ", time: " + (System.currentTimeMillis() - putStart) + ", putId: " + putId);
        }
        currentT[threadId.get()] = (int) message.getT();

        if ((putId + 1) % MAX_MEM_TABLE_SIZE == 0) {
//            logger.info("Submit memTable persist task, putId: " + putId);
            int currentMinT = currentT[0];
            for (int i = 1; i < currentT.length; i++) {
                currentMinT = Math.min(currentMinT, currentT[i]);
            }
            int finalCurrentMinT = currentMinT;

            NavigableMap<Long, Message> frozenMemTable = memTable;
            memTable = new TreeMap<>((Collections.reverseOrder()));

            persistThreadPool.execute(() -> persistMemTable(frozenMemTable, finalCurrentMinT));
//            logger.info("Submitted memTable persist task, time: "
//                    + (System.currentTimeMillis() - putStart) + ", putId: " + putId);
        }
    }

    private void persistMemTable(Map<Long, Message> frozenMemTable, int currentMinT) {
        long persistStart = System.currentTimeMillis();
        int persistId = persistCounter.getAndIncrement();
        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable with size: " + frozenMemTable.size()
                    + ", buffer index: " + bufferIndex + ", persistId: " + persistId);
        }

        Message[] sourceBuffer = persistId % 2 == 0? msgBuffer1 : msgBuffer2;
        Message[] targetBuffer = persistId % 2 == 1? msgBuffer1 : msgBuffer2;

        int i = 0;
        int j = 0;
        for (Message message : frozenMemTable.values()) {
            while (i < bufferIndex && sourceBuffer[i].getT() >= message.getT()) {
                targetBuffer[j++] = sourceBuffer[i++];
            }
            targetBuffer[j++] = message;
        }
        while (i < bufferIndex) {
            targetBuffer[j++] = sourceBuffer[i++];
        }
        bufferIndex = j;

        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Copied memTable with size: " + frozenMemTable.size() + " to buffer with index: " + bufferIndex
                    + ", time: " + (System.currentTimeMillis() - persistStart) + ", persistId: " + persistId);
        }
        if (bufferIndex > 0) {
            int lastT = (int) targetBuffer[bufferIndex - 1].getT();
            int aCount = 0;
            int index = bufferIndex - 1;
            while (true) {
                Message msg = index >= 0 ? targetBuffer[index] : sentinelMessage;
                int t = (int) msg.getT();
                short a = (short) (msg.getA() - t - A_DIFF_BASE_OFFSET);

                if (t != lastT) {
                    // update t index
                    byte id = tIndexDictCount2Id[aCount];
                    if (id == 0) {
                        id = tIndexDictId++;
                        tIndexDictCount2Id[aCount] = id;
                        tIndexDictId2Count[id] = aCount;
//                        logger.info("Set t index dict: " + aCount + " -> " + id);
                    }
                    tIndex[lastT] = id;

                    // sort and store a (diff)
                    Arrays.sort(aBuffer, 0, aCount);
                    for (int k = 0; k < aCount; k++) {
                        if (msgCounter < A_DIFF_HALF_SIZE) {
                            aFirstHalf[msgCounter] = aBuffer[k];
                        } else {
                            aLastHalf.putShort(aBuffer[k]);
                        }
                        msgCounter++;
                    }

                    // update t index summary
                    if (t % T_INDEX_SUMMARY_RATE == 0) {
                        tSummary[t / T_INDEX_SUMMARY_RATE] = msgCounter;
                    }
                    aCount = 0;
                }


                lastT = t;
                aBuffer[aCount++] = a;

                if (t >= currentMinT) {
                    break;
                }

                // persist body
                if (!bodyByteBufferForWrite.hasRemaining()) {
                    flushBuffer(bodyFileChannel, bodyByteBufferForWrite);
                }
                bodyByteBufferForWrite.put(msg.getBody());
                index--;
            }
            bufferIndex = index + 1;
        }
        if (persistId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Done persisting memTable with size: " + frozenMemTable.size()
                    + ", buffer index: " + bufferIndex
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
            persistMemTable(memTable, Integer.MAX_VALUE);
            flushBuffer(bodyFileChannel, bodyByteBufferForWrite);
            persistDone = true;
            persistThreadPool.shutdown();
            logger.info("Flushed all memTables, time: " + (System.currentTimeMillis() - getStart));

            msgBuffer1 = null;
            msgBuffer2 = null;
            System.gc();
            logger.info("Try active GC, time: " + (System.currentTimeMillis() - getStart));
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

        int aIndex = tSummary[(int) (tMin / T_INDEX_SUMMARY_RATE)];
        for (int t = (int) (tMin / T_INDEX_SUMMARY_RATE * T_INDEX_SUMMARY_RATE); t < tMin; t++) {
            aIndex += tIndexDictId2Count[tIndex[t]];
        }

        ByteBuffer bodyByteBufferForRead = threadBufferForReadBody.get();
        bodyByteBufferForRead.flip();

        for (int t = (int) tMin; t <= tMax; t++) {
            int aCount = tIndexDictId2Count[tIndex[t]];
            if (aCount > 2) {
                long curMax = getA(t, aIndex + aCount - 1);
                if (curMax < aMin) {
                    aIndex += aCount;
                    bodyByteBufferForRead.position(bodyByteBufferForRead.position()
                            + Math.min(bodyByteBufferForRead.remaining(), aCount * BODY_BYTE_LENGTH));
                    continue;
                }
            }
            while (aCount > 0) {
                long a = getA(t, aIndex);
                if (a > aMax) {
                    aIndex += aCount;
                    bodyByteBufferForRead.position(bodyByteBufferForRead.position()
                            + Math.min(bodyByteBufferForRead.remaining(), aCount * BODY_BYTE_LENGTH));
                    break;
                }
                if (a < aMin) {
                    bodyByteBufferForRead.position(bodyByteBufferForRead.position() + BODY_BYTE_LENGTH);
                } else {
                    if (!bodyByteBufferForRead.hasRemaining()) {
                        try {
                            bodyByteBufferForRead.clear();
                            bodyFileChannel.read(bodyByteBufferForRead, (long) aIndex * BODY_BYTE_LENGTH);
                            bodyByteBufferForRead.flip();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    byte[] body = new byte[BODY_BYTE_LENGTH];
                    bodyByteBufferForRead.get(body);
                    Message msg = new Message(a, t, body);
                    result.add(msg);
                }

                aIndex++;
                aCount--;
            }
        }

        bodyByteBufferForRead.clear();

        if (IS_TEST_RUN) {
            getMsgCounter.addAndGet(result.size());
        }
        if (getId % GET_SAMPLE_RATE == 0) {
//            for (int i = 0; i < Math.min(100, result.size()); i++) {
//                logger.info(" result : t - " + result.get(i).getT() + ", a - " + result.get(i).getA());
//            }
            logger.info("Return sorted result with size: " + result.size()
                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
        }
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        long avgStart = System.currentTimeMillis();
//        int avgId = avgCounter.getAndIncrement();
//        if (IS_TEST_RUN && avgId == 0) {
//            _getEnd = System.currentTimeMillis();
//            _avgStart = _getEnd;
//        }
//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("getAvgValue - tMin: " + tMin + ", tMax: " + tMax
//                    + ", aMin: " + aMin + ", aMax: " + aMax + ", avgId: " + avgId);
//            if (IS_TEST_RUN && avgId == TEST_BOUNDARY) {
//                long putDuration = _putEnd - _putStart;
//                long getDuration = _getEnd - _getStart;
//                long avgDuration = System.currentTimeMillis() - _avgStart;
//                int putScore = (int) (putCounter.get() / putDuration);
//                int getScore = (int) (getMsgCounter.get() / getDuration);
//                int avgScore = (int) (avgMsgCounter.get() / avgDuration);
//                int totalScore = putScore + getScore + avgScore;
//                logger.info("Test result: \n"
//                        + "\tput: " + putCounter.get() + " / " + putDuration + "ms = " + putScore + "\n"
//                        + "\tget: " + getMsgCounter.get() + " / " + getDuration + "ms = " + getScore + "\n"
//                        + "\tavg: " + avgMsgCounter.get() + " / " + avgDuration + "ms = " + avgScore + "\n"
//                        + "\ttotal: " + totalScore + "\n"
//                );
//                throw new RuntimeException(putScore + "/" + getScore + "/" + avgScore);
//            }
//        }
        long sum = 0;
        int count = 0;

        int aIndex = tSummary[(int) (tMin / T_INDEX_SUMMARY_RATE)];
        for (int t = (int) (tMin / T_INDEX_SUMMARY_RATE * T_INDEX_SUMMARY_RATE); t < tMin; t++) {
            aIndex += tIndexDictId2Count[tIndex[t]];
        }

        for (int t = (int) tMin; t <= tMax; t++) {
            int aCount = tIndexDictId2Count[tIndex[t]];
            if (aCount > 2) {
                long curMax = getA(t, aIndex + aCount - 1);
                if (curMax < aMin) {
                    aIndex += aCount;
                    continue;
                }
            }
            while (aCount > 0) {
                long a = getA(t, aIndex);
                if (a > aMax) {
                    aIndex += aCount;
                    break;
                }
                if (a >= aMin) {
                    sum += a;
                    count++;
                }
                aIndex++;
                aCount--;
            }
        }

//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("Got " + count // + ", skip: " + skip
//                    + ", time: " + (System.currentTimeMillis() - avgStart));
//        }
//        if (IS_TEST_RUN) {
//            avgMsgCounter.addAndGet((int) count);
//        }
        return count == 0 ? 0 : sum / count;
    }

    private long getA(long t, int index) {
        long aDiff;
        if (index < A_DIFF_HALF_SIZE) {
            aDiff = aFirstHalf[index];
        } else {
            aDiff = aLastHalf.getShort((index - A_DIFF_HALF_SIZE) * KEY_A_BYTE_LENGTH);
        }
        return aDiff + t + A_DIFF_BASE_OFFSET;
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
