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
import java.util.stream.Collectors;

import static io.openmessaging.Constants.*;

/**
 * @author .ignore 2019-07-29
 */
@SuppressWarnings("DuplicatedCode")
public class LsmMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(LsmMessageStoreImpl.class);

    private static final int MAX_MEM_TABLE_SIZE = 100000;

    private static final int SST_FILE_INDEX_RATE = 32;

    private static final int DIFF_A_BASE_OFFSET = 10000;

    private static final int T_INDEX_SIZE = 1024 * 1024 * 1024;
    private static final int T_INDEX_SUMMARY_RATE = 32;

    private static final int WRITE_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1024;
    private static final int READ_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1024;

    private static final int WRITE_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;

    private static final int PERSIST_SAMPLE_RATE = 100;
    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

//    private static final int PERSIST_SAMPLE_RATE = 10000000;
//    private static final int PUT_SAMPLE_RATE = 10000000;
//    private static final int GET_SAMPLE_RATE = 10000000;
//    private static final int AVG_SAMPLE_RATE = 10000000;

    private static RandomAccessFile aFile;
    private static ByteBuffer aByteBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);

    static {
        logger.info("LsmMessageStoreImpl loaded");

        try {
            aFile = new RandomAccessFile(Constants.DATA_DIR + "a.data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private volatile NavigableMap<Long, Message> memTable = new TreeMap<>();

    private volatile NavigableMap<Long, Message> lastFrozenTable = null;

    private ThreadPoolExecutor persistThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger fileCounter = new AtomicInteger(0);
    private volatile boolean persistDone = false;

    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);

    private List<SSTableFile> ssTableFileList = new ArrayList<>();

    private short[] tIndex = new short[T_INDEX_SIZE];
    private int[] tSummary = new int[T_INDEX_SIZE / T_INDEX_SUMMARY_RATE];
    private AtomicInteger aCounter = new AtomicInteger(0);

    private ThreadLocal<ByteBuffer> threadBufferForMsg = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BUFFER_SIZE));

    private ThreadLocal<ByteBuffer> threadBufferForWrite = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE));

    private ThreadLocal<ByteBuffer> threadBufferForReadA = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A_BUFFER_SIZE));

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);
    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;

    @Override
    public void put(Message message) {
        long putStart = System.currentTimeMillis();
        int putId = putCounter.getAndIncrement();
        if (IS_TEST_RUN && putId == 0) {
            _putStart = System.currentTimeMillis();
        }
        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("putMessage - t: " + message.getT() + ", a: " + message.getA() + ", putId: " + putId);
        }
        long key = (message.getT() << 32) + putId;

        synchronized (this) {
            memTable.put(key, message);
        }
        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("putMessage to memTable with key " + key + ", time: "
                    + (System.currentTimeMillis() - putStart) + ", putId: " + putId);
        }

        if ((putId + 1) % MAX_MEM_TABLE_SIZE == 0) {
//            logger.info("Submit memTable persist task, putId: " + putId);
            NavigableMap<Long, Message> frozenMemTable = memTable;
            memTable = new TreeMap<>();
            persistThreadPool.execute(() -> persistMemTable(frozenMemTable));
//            logger.info("Submitted memTable persist task, time: "
//                    + (System.currentTimeMillis() - putStart) + ", putId: " + putId);
        }
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
            if (memTable.size() > 0) {
                persistMemTable(memTable);
            }
            flushMemBuffer();
            sortSSTableList();
            persistDone = true;
//            persistThreadPool.shutdown();
            printSSTableList();
            logger.info("Flushed all memTables, time: " + (System.currentTimeMillis() - getStart));
//            persistThreadPool.execute(() -> buildMemoryIndex());
        }
        while (!persistDone) {
            logger.info("Waiting for all persist tasks to finish");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("All persist tasks has finished, time: " + (System.currentTimeMillis() - getStart));
        }

        List<SSTableFile> targetFileList = findTargetFileList(ssTableFileList, tMin, tMax);
        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("Found target files list: " + targetFileList.stream().map(s -> s.id).collect(Collectors.toList())
                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
        }

        ArrayList<Message> result = new ArrayList<>(1024);
        ByteBuffer bufferForMsg = threadBufferForMsg.get();

        for (SSTableFile file : targetFileList) {
            try {
                int index = findStartIndex(file, tMin, tMax);
                if (getId % GET_SAMPLE_RATE == 0) {
                    logger.info("Found index: " + index + " for file: " + file.id
                            + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
                }

                int offset = index * Constants.MSG_BYTE_LENGTH;
                boolean allFound = false;

                while (!allFound && offset < file.size) {
                    offset += file.channel.read(bufferForMsg, offset);
                    bufferForMsg.flip();

                    while (bufferForMsg.remaining() > 0) {
                        long t = bufferForMsg.getInt();
                        if (t < tMin) {
                            bufferForMsg.position(bufferForMsg.position()
                                    + Constants.KEY_A_BYTE_LENGTH + Constants.BODY_BYTE_LENGTH);
                            continue;
                        }
                        if (t > tMax) {
                            allFound = true;
                            break;
                        }
                        long a = bufferForMsg.getShort() + t + DIFF_A_BASE_OFFSET;
                        if (a >= aMin && a <= aMax) {
                            byte[] body = new byte[Constants.BODY_BYTE_LENGTH];
                            bufferForMsg.get(body);
                            Message msg = new Message(a, t, body);
                            result.add(msg);
                        } else {
                            bufferForMsg.position(bufferForMsg.position() + Constants.BODY_BYTE_LENGTH);
                        }
                    }
                    bufferForMsg.clear();
                }
                if (getId % GET_SAMPLE_RATE == 0) {
                    logger.info("Found total " + result.size() + " msgs for file: " + file.id
                            + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (targetFileList.size() > 1) {
            result.sort((o1, o2) -> (int) (o1.getT() - o2.getT()));
        }
//        printGetResult(result);

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
        if (avgId != 0) {
//            try {
//                Thread.sleep(1000000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
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
        long count = 0;

        int offset = tSummary[(int) (tMin / T_INDEX_SUMMARY_RATE)];
        for (int t = (int) (tMin / T_INDEX_SUMMARY_RATE * T_INDEX_SUMMARY_RATE); t < tMin; t++) {
            offset += tIndex[t];
        }
        offset *= KEY_A_BYTE_LENGTH;
//        logger.info("offset: " + offset);

        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        aByteBufferForRead.clear();
        aByteBufferForRead.flip();

        for (int t = (int) tMin; t <= tMax; t++) {
            int aCount = tIndex[t];
//            logger.info("tIndex on " + t + ": " + aCount);
            while (aCount-- > 0) {
                if (aByteBufferForRead.remaining() == 0) {
                    int bytes = 0;
                    try {
                        aByteBufferForRead.clear();
                        bytes = aFile.getChannel().read(aByteBufferForRead, offset);
                        aByteBufferForRead.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    offset += bytes;
                }
                long a = aByteBufferForRead.getShort() + t + DIFF_A_BASE_OFFSET;
//                logger.info("a: " + a);
                if (a != t) {
                    logger.error("t: " + t + ", a: " + a + ", pos: " + offset);
                    throw new RuntimeException();
                }
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }
            }
        }
        aByteBufferForRead.clear();

        if (IS_TEST_RUN) {
            avgMsgCounter.addAndGet((int) count);
        }
        return count == 0 ? 0 : sum / count;
    }

    private int findStartIndex(SSTableFile file, long tMin, long tMax) throws IOException {
        if (tMin <= file.tStart) {
            return 0;
        }
        int[] fileIndexList = file.fileIndexList;
        int start = 0;
        int end = fileIndexList.length - 1;
        while (start < end) {
            int index = (start + end) / 2;
            int t = fileIndexList[index];
            if (t < tMin) {
                start = index + 1;
            } else {
                end = index;
            }
        }
        return start > 0 ? (start - 1) * SST_FILE_INDEX_RATE : 0;
    }

    private List<SSTableFile> findTargetFileList(List<SSTableFile> sourceFileList, long tMin, long tMax) {
        List<SSTableFile> targetFileList = new ArrayList<>();
        int index;
        int start = 0;
        int end = sourceFileList.size() - 1;
        while (start < end) {
            index = (start + end) / 2;
            long fileEnd = sourceFileList.get(index).tEnd;
            if (fileEnd < tMin) {
                start = index + 1;
            } else {
                end = index;
            }
        }
        index = start;
        for (int i = index; i < Math.min(i + 10, sourceFileList.size()); i++) {
            SSTableFile file = sourceFileList.get(i);
            if (tMax >= file.tStart && tMin <= file.tEnd) {
                targetFileList.add(file);
            }
        }
        return targetFileList;
    }

    private void persistMemTable(NavigableMap<Long, Message> frozenMemTable) {
        long persistStart = System.currentTimeMillis();
        int fileId = fileCounter.getAndIncrement();
        String fileName = Constants.DATA_DIR + "sst" + fileId + ".data";
        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable to file: " + fileName);
        }

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException e) {
            logger.info("[ERROR] File not found", e);
            throw new RuntimeException(e);
        }

        ByteBuffer buffer = threadBufferForWrite.get();
        FileChannel channel = raf.getChannel();
        int[] fileIndexList = new int[(frozenMemTable.size() - 1) / SST_FILE_INDEX_RATE + 1];
        int writeCount = 0;
        int writeBytes = 0;

        for (Map.Entry<Long, Message> entry : frozenMemTable.entrySet()) {
            Message msg = entry.getValue();
            if (buffer.remaining() < Constants.MSG_BYTE_LENGTH) {
                buffer.flip();
                try {
                    writeBytes += buffer.limit();
                    channel.write(buffer);
                } catch (IOException e) {
                    logger.info("[ERROR] Write to channel failed: " + e.getMessage());
                }
                buffer.clear();
            }
            int t = (int) msg.getT();
            short a = (short) (msg.getA() - msg.getT() - DIFF_A_BASE_OFFSET);

            buffer.putInt(t);
            buffer.putShort(a);
            buffer.put(msg.getBody());
            if (writeCount % SST_FILE_INDEX_RATE == 0) {
                fileIndexList[writeCount / SST_FILE_INDEX_RATE] = (int) msg.getT();
            }
            writeCount++;
        }

        if (buffer.position() > 0) {
            buffer.flip();
            try {
                writeBytes += buffer.limit();
                channel.write(buffer);
            } catch (IOException e) {
                logger.info("ERROR write to file channel: " + e.getMessage());
            }
            buffer.clear();
        }

        long minT = frozenMemTable.firstEntry().getValue().getT();
        long maxT = frozenMemTable.lastEntry().getValue().getT();

        ssTableFileList.add(new SSTableFile(fileId, raf, channel, minT, maxT, fileIndexList));

        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Done persisting memTable to file: " + fileName
                    + ", written msgs: " + writeCount + ", written bytes: " + writeBytes
                    + ", index size: " + fileIndexList.length + ", time: " + (System.currentTimeMillis() - persistStart));
        }

        if (lastFrozenTable != null) {
            int lastT = -1;
            for (Map.Entry<Long, Message> entry : lastFrozenTable.entrySet()) {
                Message msg = entry.getValue();
                int t = (int) msg.getT();
                if (t < minT) {
                    tIndex[t]++;
                    if (aByteBufferForWrite.remaining() < Constants.KEY_A_BYTE_LENGTH) {
                        aByteBufferForWrite.flip();
                        try {
                            aFile.getChannel().write(aByteBufferForWrite);
                        } catch (IOException e) {
                            logger.info("[ERROR] Write to channel failed: " + e.getMessage());
                        }
                        aByteBufferForWrite.clear();
                    }
                    aByteBufferForWrite.putShort((short) (msg.getA() - t - DIFF_A_BASE_OFFSET));

                    if (t != lastT && t % T_INDEX_SUMMARY_RATE == 0) {
                        tSummary[t / T_INDEX_SUMMARY_RATE] = aCounter.get();
                    }
                    aCounter.getAndIncrement();
                } else {
                    frozenMemTable.put(entry.getKey(), msg);
                }
                lastT = t;
            }
            if (aByteBufferForWrite.position() > 0) {
                aByteBufferForWrite.flip();
                try {
                    aFile.getChannel().write(aByteBufferForWrite);
                } catch (IOException e) {
                    logger.info("[ERROR] Write to channel failed: " + e.getMessage());
                }
                aByteBufferForWrite.clear();
            }
        }
        lastFrozenTable = frozenMemTable;
    }

    private void flushMemBuffer() {
        int lastT = -1;
        for (Map.Entry<Long, Message> entry : lastFrozenTable.entrySet()) {
            Message msg = entry.getValue();
            int t = (int) msg.getT();
            tIndex[t]++;
            if (aByteBufferForWrite.remaining() < Constants.KEY_A_BYTE_LENGTH) {
                aByteBufferForWrite.flip();
                try {
                    aFile.getChannel().write(aByteBufferForWrite);
                } catch (IOException e) {
                    logger.info("[ERROR] Write to channel failed: " + e.getMessage());
                }
                aByteBufferForWrite.clear();
            }
            aByteBufferForWrite.putShort((short) (msg.getA() - t - DIFF_A_BASE_OFFSET));

            if (t != lastT && t % T_INDEX_SUMMARY_RATE == 0) {
                tSummary[t / T_INDEX_SUMMARY_RATE] = aCounter.get();
            }
            aCounter.getAndIncrement();
            lastT = t;
        }
        if (aByteBufferForWrite.position() > 0) {
            aByteBufferForWrite.flip();
            try {
                aFile.getChannel().write(aByteBufferForWrite);
            } catch (IOException e) {
                logger.info("[ERROR] Write to channel failed: " + e.getMessage());
            }
            aByteBufferForWrite.clear();
        }

//            long count = 0;
//            for (int i = 0; i < 1000000; i++) {
//                if (i % T_INDEX_SUMMARY_RATE == 0) {
//                    assert count == tSummary[i / T_INDEX_SUMMARY_RATE];
//                }
//                count += tIndex[i];
//            }
//
//        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
//        aByteBufferForRead.clear();
//        int offset = 0;
//        while (true) {
//            try {
//                int bytes = aFile.getChannel().read(aByteBufferForRead, offset);
//                if (bytes <= 0) {
//                    logger.info("AAA offset: " + offset);
//                    break;
//                }
//                offset += bytes;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            aByteBufferForRead.flip();
//            while (aByteBufferForRead.remaining() > 0) {
//                try {
//                    if (aByteBufferForRead.getShort() + DIFF_A_BASE_OFFSET != 0) {
//                        throw new RuntimeException("offset: " + offset);
//                    }
//                } catch (RuntimeException e) {
//                    logger.error(e);
//
//                }
//            }
//            aByteBufferForRead.clear();
//        }
    }

//    private void buildMemoryIndex() {
//        long buildStart = System.currentTimeMillis();
//        logger.info("Start building memory index");
//    }

    private static class SSTableFile {
        RandomAccessFile file;
        FileChannel channel;
        int id = -1;
        long tStart = 0;
        long tEnd = 0;
        int size = 0;
        int msgs = 0;
        int[] fileIndexList;

        SSTableFile(int id, RandomAccessFile file, FileChannel channel, long tStart, long tEnd, int[] fileIndexList) {
            this.id = id;
            this.file = file;
            this.channel = channel;
            this.tStart = tStart;
            this.tEnd = tEnd;
            try {
                this.size = (int) channel.size();
                this.msgs = size / Constants.MSG_BYTE_LENGTH;
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.fileIndexList = fileIndexList;
        }
    }

    private void sortSSTableList() {
        ssTableFileList.sort(((o1, o2) -> (int) (o1.tEnd - o2.tEnd)));
    }

    private void printSSTableList() {
        logger.info("SSTableFile list with size: " + ssTableFileList.size());
        for (SSTableFile file : ssTableFileList.subList(0, Math.min(10, ssTableFileList.size()))) {
            try {
                logger.info(file.id + "\t[" + file.tStart + ", " + file.tEnd + "] - "
                        + file.channel.size() + " / " + Constants.MSG_BYTE_LENGTH + " = " + file.msgs);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void printGetResult(List<Message> result) {
        logger.info("Get result with size: " + result.size());
        for (Message m : result.subList(0, Math.min(result.size(), 10))) {
            logger.info("m.t: " + m.getT() + ", m.a: " + m.getA());
        }
    }
}
