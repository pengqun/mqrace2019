package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static io.openmessaging.Constants.*;

/**
 * @author .ignore 2019-07-29
 */
public class LsmMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(LsmMessageStoreImpl.class);

    private static final int SST_FILE_INDEX_RATE = 32;

    private static final int WRITE_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1024;
    private static final int READ_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1024;
    private static final int WRITE_TA_BUFFER_SIZE = Constants.TA_BYTE_LENGTH * 1024;
    private static final int READ_TA_BUFFER_SIZE = Constants.TA_BYTE_LENGTH * 1024;

    private static final int PERSIST_SAMPLE_RATE = 100;
    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    static {
        logger.info("LsmMessageStoreImpl loaded");
    }

    private static final int RING_BUFFER_SIZE = 10000000;
    private static final int PERSIST_SEGMENT_SIZE = 100000;

//    private volatile NavigableMap<Long, Message> memTable = new TreeMap<>();
//    private volatile NavigableMap<Long, Message> memTable = new ConcurrentSkipListMap<>();
    private Message[] ringBuffer = new Message[RING_BUFFER_SIZE];

    private ThreadPoolExecutor persistThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger fileCounter = new AtomicInteger(0);
    private volatile boolean persistDone = false;

    private AtomicInteger writeCounter = new AtomicInteger(0);

    private AtomicInteger getCounter = new AtomicInteger(0);
    private AtomicInteger avgCounter = new AtomicInteger(0);

    private List<SSTableFile> ssTableFileList = new ArrayList<>();
    private List<SSTableFile> ssTableFileListTa = new ArrayList<>();

    private ThreadLocal<ByteBuffer> threadBufferForMsg = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForMsgTa = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_TA_BUFFER_SIZE));

    private ThreadLocal<ByteBuffer> threadBufferForWrite = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE));
    private ThreadLocal<ByteBuffer> threadBufferForWriteTa = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(WRITE_TA_BUFFER_SIZE));

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);
    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;

    private final Object lock = new Object();
    private boolean[] doneFlags = new boolean[30000];

    @Override
    public void put(Message message) {
        long putStart = System.currentTimeMillis();
        int putId = putCounter.getAndIncrement();
        if (IS_TEST_RUN && putId == 0) {
            _putStart = System.currentTimeMillis();
        }
        if (IS_TEST_RUN && putId == 10000 * 10000) {
            throw new RuntimeException("" + (System.currentTimeMillis() - _putStart));
        }
        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("putMessage - t: " + message.getT() + ", a: " + message.getA() + ", putId: " + putId);
        }

//        while (putId >= writeCounter.get() + RING_BUFFER_SIZE) {
//            logger.info("Waiting for buffer to be available");
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        int index = putId % RING_BUFFER_SIZE;
        ringBuffer[index] = message;

        if (putId % PUT_SAMPLE_RATE == 0) {
            logger.info("putMessage to buffer with index " + index + ", time: "
                    + (System.currentTimeMillis() - putStart) + ", putId: " + putId);
        }

        int segId = putId % PERSIST_SEGMENT_SIZE;
        if (putId >= PERSIST_SEGMENT_SIZE && segId < PRODUCER_THREAD_NUM) {
            int taskId = putId / PERSIST_SEGMENT_SIZE;
            if (segId == 0) {
                int start = (index + RING_BUFFER_SIZE - PERSIST_SEGMENT_SIZE) % RING_BUFFER_SIZE;
                int end = start + PERSIST_SEGMENT_SIZE;
                persistMemTable(start, end);
                synchronized (lock) {
                    doneFlags[taskId] = true;
                    lock.notifyAll();
                    logger.info("Done " + taskId + " in " + putId);
                }
            } else {
                synchronized (lock) {
                    while (!doneFlags[taskId]) {
                        logger.info("Waiting for " + taskId + " in " + putId);
                        try {
                            lock.wait(5000);
                            if (!doneFlags[taskId]) {
                                throw new RuntimeException("timeout");
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
//            persistThreadPool.execute(() -> persistMemTable(start, end));
        }
    }

    private void persistMemTable(int start, int end) {
        long persistStart = System.currentTimeMillis();
        int fileId = fileCounter.getAndIncrement();
        String fileName = Constants.DATA_DIR + "sst" + fileId + ".data";
        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable to file: " + fileName);
        }
        logger.info("persist: " + start + " to " + end);

        Arrays.sort(ringBuffer, start, end, Comparator.comparingLong(Message::getT));

        String fileNameTa = Constants.DATA_DIR + "ssta" + fileId + ".data";

        RandomAccessFile raf = null;
        RandomAccessFile rafTa = null;
        try {
            raf = new RandomAccessFile(fileName, "rw");
            rafTa = new RandomAccessFile(fileNameTa, "rw");
        } catch (FileNotFoundException e) {
            logger.info("[ERROR] File not found", e);
            throw new RuntimeException(e);
        }

        ByteBuffer buffer = threadBufferForWrite.get();
        ByteBuffer bufferTa = threadBufferForWriteTa.get();

        FileChannel channel = raf.getChannel();
        FileChannel channelTa = rafTa.getChannel();
        long[] fileIndexList = new long[(PERSIST_SEGMENT_SIZE - 1) / SST_FILE_INDEX_RATE + 1];
        int writeCount = 0;
        int writeBytes = 0;

        for (int i = start; i < end; i++) {
            Message msg = ringBuffer[i];
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
            if (bufferTa.remaining() < Constants.TA_BYTE_LENGTH) {
                bufferTa.flip();
                try {
                    channelTa.write(bufferTa);
                } catch (IOException e) {
                    logger.info("[ERROR] Write to channel failed: " + e.getMessage());
                }
                bufferTa.clear();
            }
            buffer.putLong(msg.getT());
            buffer.putLong(msg.getA());
            buffer.put(msg.getBody());
            bufferTa.putLong(msg.getT());
            bufferTa.putLong(msg.getA());
            if (writeCount % SST_FILE_INDEX_RATE == 0) {
                fileIndexList[writeCount / SST_FILE_INDEX_RATE] = msg.getT();
            }
            writeCount++;
        }

        buffer.flip();
        try {
            writeBytes += buffer.limit();
            channel.write(buffer);
        } catch (IOException e) {
            logger.info("ERROR write to file channel: " + e.getMessage());
        }
        buffer.clear();
        bufferTa.flip();
        try {
            channelTa.write(bufferTa);
        } catch (IOException e) {
            logger.info("ERROR write to file channel: " + e.getMessage());
        }
        bufferTa.clear();

        long minT = ringBuffer[start].getT();
        long maxT = ringBuffer[end - 1].getT();

        ssTableFileList.add(new SSTableFile(fileId, raf, channel, minT, maxT, fileIndexList));
        ssTableFileListTa.add(new SSTableFile(fileId, rafTa, channelTa, minT, maxT, fileIndexList));

        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Done persisting memTable to file: " + fileName
                    + ", written msgs: " + writeCount + ", written bytes: " + writeBytes
                    + ", index size: " + fileIndexList.length + ", time: " + (System.currentTimeMillis() - persistStart));
        }
        writeCounter.addAndGet(writeCount);
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

            int index = putCounter.get() % RING_BUFFER_SIZE;
            if (index == 0) {
                int start = RING_BUFFER_SIZE - PERSIST_SEGMENT_SIZE;
                int end = start + PERSIST_SEGMENT_SIZE;
                persistMemTable(start, end);
            } else {
                int start = ((index - 1) / PERSIST_SEGMENT_SIZE) * PERSIST_SEGMENT_SIZE;
                int end = index;
                persistMemTable(start, end);
            }

            sortSSTableList();
            persistDone = true;
            persistThreadPool.shutdown();
            printSSTableList();
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

        List<SSTableFile> targetFileList = findTargetFileList(ssTableFileList, tMin, tMax);
        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("Found target files list: " + targetFileList.stream().map(s -> s.id).collect(Collectors.toList())
                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
        }

        ArrayList<Message> result = new ArrayList<>(4096);
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
                        long t = bufferForMsg.getLong();
                        if (t < tMin) {
                            bufferForMsg.position(bufferForMsg.position()
                                    + Constants.KEY_A_BYTE_LENGTH + Constants.BODY_BYTE_LENGTH);
                            continue;
                        }
                        if (t > tMax) {
                            allFound = true;
                            break;
                        }
                        long a = bufferForMsg.getLong();
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
            result.sort(Comparator.comparingLong(Message::getT));
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

        List<SSTableFile> targetFileList = findTargetFileList(ssTableFileListTa, tMin, tMax);
        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("Found target files list: " + targetFileList.stream().map(s -> s.id).collect(Collectors.toList())
                    + ", time: " + (System.currentTimeMillis() - avgStart) + ", avgId: " + avgId);
        }

        ByteBuffer bufferForMsg = threadBufferForMsgTa.get();

        for (SSTableFile file : targetFileList) {
            FileChannel fileChannel = file.channel;
            try {
                int index = findStartIndex(file, tMin, tMax);
                if (avgId % AVG_SAMPLE_RATE == 0) {
                    logger.info("Found index: " + index + " for file: " + file.id
                            + ", time: " + (System.currentTimeMillis() - avgStart) + ", avgId: " + avgId);
                }

                int offset = index * Constants.TA_BYTE_LENGTH;
                boolean allFound = false;

                while (!allFound && offset < file.size) {
                    offset += fileChannel.read(bufferForMsg, offset);
                    bufferForMsg.flip();

                    while (bufferForMsg.remaining() > 0) {
                        long t = bufferForMsg.getLong();
                        if (t < tMin) {
                            bufferForMsg.position(bufferForMsg.position() + Constants.KEY_A_BYTE_LENGTH);
                            continue;
                        }
                        if (t > tMax) {
                            allFound = true;
                            break;
                        }
                        long a = bufferForMsg.getLong();
                        if (a >= aMin && a <= aMax) {
                            sum += a;
                            count++;
                        }
                    }
                    bufferForMsg.clear();
                }
                if (avgId % GET_SAMPLE_RATE == 0) {
                    logger.info("Found total " + count + " msgs for file: " + file.id
                            + ", time: " + (System.currentTimeMillis() - avgStart) + ", avgId: " + avgId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (IS_TEST_RUN) {
            avgMsgCounter.addAndGet((int) count);
        }
        return count == 0 ? 0 : sum / count;
    }

    private int findStartIndex(SSTableFile file, long tMin, long tMax) throws IOException {
        if (tMin <= file.tStart) {
            return 0;
        }
        long[] fileIndexList = file.fileIndexList;
        int start = 0;
        int end = fileIndexList.length - 1;
        while (start < end) {
            int index = (start + end) / 2;
            long t = fileIndexList[index];
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

    private static class SSTableFile {
        RandomAccessFile file;
        FileChannel channel;
        int id = -1;
        long tStart = 0;
        long tEnd = 0;
        int size = 0;
        int msgs = 0;
        long[] fileIndexList;

        SSTableFile(int id, RandomAccessFile file, FileChannel channel, long tStart, long tEnd, long[] fileIndexList) {
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
        ssTableFileList.sort(Comparator.comparingLong((o) -> o.tEnd));
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
