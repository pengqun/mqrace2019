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

    private static final int MAX_MEM_TABLE_SIZE = 10 * 10000;

    private static final int SST_FILE_INDEX_RATE = 32;

    private static final int DIFF_A_BASE_OFFSET = 10000;

    private static final int T_INDEX_SIZE = 1024 * 1024 * 1024;
    private static final int T_INDEX_SUMMARY_RATE = 32;

    private static final int T_WRITE_ARRAY_SIZE = 200 * 10000;

    private static final int WRITE_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1024;
    private static final int READ_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1024;

    private static final int WRITE_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 1024;
    private static final int READ_A_BUFFER_SIZE = Constants.KEY_A_BYTE_LENGTH * 2048;

    private static final int PERSIST_SAMPLE_RATE = 100;
    private static final int PUT_SAMPLE_RATE = 10000000;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    private static FileChannel aFileChannel;
    private static ByteBuffer aByteBufferForWrite = ByteBuffer.allocateDirect(WRITE_A_BUFFER_SIZE);

    static {
        logger.info("LsmMessageStoreImpl loaded");

        try {
            RandomAccessFile aFile = new RandomAccessFile(Constants.DATA_DIR + "a.data", "rw");
            aFileChannel = aFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private volatile NavigableMap<Long, Message> memTable = new TreeMap<>();

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

    private Long[] taBuffer = new Long[T_WRITE_ARRAY_SIZE];
    private int taIndex = 0;

    private int[] currentT = new int[PRODUCE_THREAD_NUM];

    private ThreadLocal<ByteBuffer> threadBufferForMsg = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_BUFFER_SIZE));

    private ThreadLocal<ByteBuffer> threadBufferForWrite = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE));

    private ThreadLocal<ByteBuffer> threadBufferForReadA = ThreadLocal.withInitial(()
            -> ByteBuffer.allocateDirect(READ_A_BUFFER_SIZE));

    private AtomicInteger threadIdCounter = new AtomicInteger(0);
    private ThreadLocal<Integer> threadId = ThreadLocal.withInitial(()
            -> threadIdCounter.getAndIncrement());

    private AtomicLong getMsgCounter = new AtomicLong(0);
    private AtomicLong avgMsgCounter = new AtomicLong(0);
    private long _putStart = 0;
    private long _putEnd = 0;
    private long _getStart = 0;
    private long _getEnd = 0;
    private long _avgStart = 0;

    private long _firstStart = 0;

    @Override
    public void put(Message message) {
        long putStart = System.currentTimeMillis();
        int putId = putCounter.getAndIncrement();
        if (IS_TEST_RUN && putId == 0) {
            _putStart = putStart;
            _firstStart = putStart;
        }
//        if (IS_TEST_RUN && _firstStart > 0 && (putStart - _firstStart) > 60 * 1000) {
//            logger.info("" + putStart + ", " + _firstStart);
//            throw new RuntimeException("" + putId);
//        }
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

        currentT[threadId.get()] = (int) message.getT();

        if ((putId + 1) % MAX_MEM_TABLE_SIZE == 0) {
//            logger.info("Submit memTable persist task, putId: " + putId);
            int currentMinT = currentT[0];
            for (int i = 1; i < currentT.length; i++) {
                currentMinT = Math.min(currentMinT, currentT[i]);
            }
            int finalCurrentMinT = currentMinT;

            NavigableMap<Long, Message> frozenMemTable = memTable;
            memTable = new TreeMap<>();

            persistThreadPool.execute(() -> persistMemTable(frozenMemTable, finalCurrentMinT));
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
                persistMemTable(memTable, Integer.MAX_VALUE);
            }
            flushMemBuffer();
            sortSSTableList();
            persistDone = true;
            persistThreadPool.shutdown();
            printSSTableList();
            logger.info("Flushed all memTables, time: " + (System.currentTimeMillis() - getStart));
//            persistThreadPool.execute(() -> buildMemoryIndex());

            taBuffer = null;
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
//        long skip = 0;

        long offset = tSummary[(int) (tMin / T_INDEX_SUMMARY_RATE)];
        for (int t = (int) (tMin / T_INDEX_SUMMARY_RATE * T_INDEX_SUMMARY_RATE); t < tMin; t++) {
            offset += tIndex[t];
        }
        offset *= KEY_A_BYTE_LENGTH;

        ByteBuffer aByteBufferForRead = threadBufferForReadA.get();
        aByteBufferForRead.flip();

        for (int t = (int) tMin; t <= tMax; t++) {
            int aCount = tIndex[t];
            while (aCount-- > 0) {
                if (aByteBufferForRead.remaining() == 0) {
                    try {
                        aByteBufferForRead.clear();
                        aFileChannel.read(aByteBufferForRead, offset);
                        aByteBufferForRead.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    offset += READ_A_BUFFER_SIZE;
                }
                long a = aByteBufferForRead.getShort() + t + DIFF_A_BASE_OFFSET;
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }
//                else {
//                    skip++;
//                }
            }
        }
        aByteBufferForRead.clear();

//        if (avgId % AVG_SAMPLE_RATE == 0) {
//            logger.info("Got " + count + ", skip: " + skip
//                    + ", time: " + (System.currentTimeMillis() - avgStart));
//        }
//        if (IS_TEST_RUN) {
//            avgMsgCounter.addAndGet((int) count);
//        }
        return count == 0 ? 0 : sum / count;
    }

    private int findStartIndex(SSTableFile file, long tMin, long tMax) {
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

    private void persistMemTable(NavigableMap<Long, Message> frozenMemTable, int currentMinT) {
        long persistStart = System.currentTimeMillis();
        int fileId = fileCounter.getAndIncrement();
        String fileName = Constants.DATA_DIR + "sst" + fileId + ".data";
        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable to file: " + fileName);
        }

        if (frozenMemTable.size() > 0) {
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

                taBuffer[taIndex++] = ((((long) t << 32) + msg.getA()));
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
                        + ", index size: " + fileIndexList.length + ", taIndex: " + taIndex
                        + ", time: " + (System.currentTimeMillis() - persistStart));
            }
        }

        if (taIndex > 0) {
            Arrays.sort(taBuffer, 0, taIndex, Collections.reverseOrder());
            int lastT = -1;
            int index;
            for (index = taIndex - 1; index >= 0; index--) {
                long key = taBuffer[index];
                int t = (int) (key >> 32);
                int a = (int) (key);
                if (t >= currentMinT) {
                    break;
                }
                tIndex[t]++;
                if (aByteBufferForWrite.remaining() < Constants.KEY_A_BYTE_LENGTH) {
                    aByteBufferForWrite.flip();
                    try {
                        aFileChannel.write(aByteBufferForWrite);
                    } catch (IOException e) {
                        logger.info("[ERROR] Write to channel failed: " + e.getMessage());
                    }
                    aByteBufferForWrite.clear();
                }
                aByteBufferForWrite.putShort((short) (a - t - DIFF_A_BASE_OFFSET));

                if (t != lastT && t % T_INDEX_SUMMARY_RATE == 0) {
                    tSummary[t / T_INDEX_SUMMARY_RATE] = aCounter.get();
                }
                aCounter.getAndIncrement();
                lastT = t;
            }
            taIndex = index + 1;
            if (fileId % PERSIST_SAMPLE_RATE == 0) {
                logger.info("Updated taIndex: " + taIndex);
            }
        }
    }

    private void flushMemBuffer() {
        if (aByteBufferForWrite.position() > 0) {
            aByteBufferForWrite.flip();
            try {
                aFileChannel.write(aByteBufferForWrite);
            } catch (IOException e) {
                logger.info("[ERROR] Write to channel failed: " + e.getMessage());
            }
            aByteBufferForWrite.clear();
        }
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
}
