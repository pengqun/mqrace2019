package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author .ignore 2019-07-29
 */
public class LsmMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(LsmMessageStoreImpl.class);

    private static final int MAX_MEM_TABLE_SIZE = 100000;

    static {
        logger.info("LsmMessageStoreImpl loaded");
    }

    private NavigableMap<Long, List<Message>> memTable = new TreeMap<>();
//    private volatile NavigableMap<Long, List<Message>> memTable = new ConcurrentSkipListMap<>();

    private static ThreadPoolExecutor persistThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger persistCounter = new AtomicInteger(0);
    private volatile boolean persistDone = false;

    private AtomicInteger getCounter = new AtomicInteger(0);

    private List<SSTableFile> ssTableFileList = new ArrayList<>();

    @Override
    public synchronized void put(Message message) {
        if (!memTable.containsKey(message.getT())) {
            memTable.put(message.getT(), new ArrayList<>());
        }
        memTable.get(message.getT()).add(message);

        if (putCounter.incrementAndGet() % MAX_MEM_TABLE_SIZE == 0) {
            logger.info("Submit memTable persist task");
            NavigableMap<Long, List<Message>> frozenMemTable = memTable;
            memTable = new ConcurrentSkipListMap<>();
            persistThreadPool.execute(() -> {
                persistMemTable(frozenMemTable);
            });
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (getCounter.incrementAndGet() == 1) {
            logger.info("Flush all memTables before getMessage");
            while (persistThreadPool.getActiveCount() + persistThreadPool.getQueue().size() > 0) {
                logger.info("Waiting for previous tasks to finish");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (memTable.size() > 0) {
                persistMemTable(memTable);
            }
            persistDone = true;
            persistThreadPool.shutdown();

//            logger.info("SSTableFile list: " + ssTableFileList.size());
//            for (SSTableFile file : ssTableFileList) {
//                try {
//                    logger.info("\t[" + file.tStart + ", " + file.tEnd + "] - "
//                            + file.fileChannel.size() + " / " + file.fileChannel.size() / Constants.MSG_BYTE_LENGTH);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
        }
        while (!persistDone) {
            logger.info("Waiting for all persist tasks to finish");
            try {
                Thread.sleep(100);
//                Thread.sleep(10000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("All persist tasks have finished");
        }

        ArrayList<Message> res = new ArrayList<>();
        ByteBuffer bufferForIndex = ByteBuffer.allocateDirect(8);
        long searchStart = System.currentTimeMillis();

        for (SSTableFile file : ssTableFileList) {
            if (tMax < file.tStart || tMin > file.tEnd) {
                continue;
            }
            long fileSearchStart = System.currentTimeMillis();
            FileChannel fileChannel = file.randomAccessFile.getChannel();
            try {
                int index = 0;
                int size = (int) fileChannel.size();
//                logger.info("getMessage - tMin: " + tMin + ", tMax: " + tMax);

                if (tMin > file.tStart) {
                    int start = 0;
                    int end = size / Constants.MSG_BYTE_LENGTH - 1;
                    while (start < end) {
                        index = (start + end) / 2;
                        fileChannel.read(bufferForIndex, index * Constants.MSG_BYTE_LENGTH);
                        bufferForIndex.flip();
                        long t = bufferForIndex.getLong();
//                        logger.info("Binary search t: " + t);
                        bufferForIndex.clear();
                        if (t < tMin) {
                            start = index + 1;
                        } else {
                            end = index;
                        }
                    }
                    index = start;
//                    logger.info("Found index: " + index + " in " + (System.currentTimeMillis() - fileSearchStart) + " ms");
                }

                ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.MSG_BYTE_LENGTH * 1000);
                int offset = index * Constants.MSG_BYTE_LENGTH;

                while (offset < size) {
                    int bytes = fileChannel.read(buffer, offset);
                    if (bytes <= 0) {
                        break;
                    }
                    offset += bytes;

                    buffer.flip();
                    boolean done = false;
                    while (buffer.remaining() > 0) {
                        long t = buffer.getLong();
                        if (t > tMax) {
                            done = true;
                            break;
                        }
                        long a = buffer.getLong();
                        if (a >= aMin && a <= aMax) {
                            byte[] body = new byte[Constants.BODY_BYTE_LENGTH];
                            buffer.get(body);
                            Message msg = new Message(a, t, body);
                            res.add(msg);
                        } else {
                            buffer.position(buffer.position() + Constants.BODY_BYTE_LENGTH);
                        }
                    }
                    if (done) {
                        break;
                    }
                    buffer.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        logger.info("Search end in " + (System.currentTimeMillis() - searchStart) + " ms");

        res.sort((o1, o2) -> (int) (o1.getT() - o2.getT()));
//        logger.info("Result: ");
//        for (Message m : res.subList(0, Math.min(res.size(), 100))) {
//            logger.info("m.t: " + m.getT() + ", m.a: " + m.getA());
//        }

//        logger.info("Sort end in " + (System.currentTimeMillis() - searchStart) + " ms");

//        logger.info("Done getMessage: aMin - " + aMin + ", aMax - " + aMax
//                + ", tMin - " + tMin + ", tMax - " + tMax + ", counter = " + getCounter.get());
        return res;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        long count = 0;

        ByteBuffer bufferForIndex = ByteBuffer.allocateDirect(8);

        for (SSTableFile file : ssTableFileList) {
            if (tMax < file.tStart || tMin > file.tEnd) {
                continue;
            }
            FileChannel fileChannel = file.randomAccessFile.getChannel();
            try {
                int index = 0;
                int size = (int) fileChannel.size();

                if (tMin > file.tStart) {
                    int start = 0;
                    int end = size / Constants.MSG_BYTE_LENGTH - 1;
                    while (start < end) {
                        index = (start + end) / 2;
                        fileChannel.read(bufferForIndex, index * Constants.MSG_BYTE_LENGTH);
                        bufferForIndex.flip();
                        long t = bufferForIndex.getLong();
//                        logger.info("Binary search t: " + t);
                        bufferForIndex.clear();
                        if (t < tMin) {
                            start = index + 1;
                        } else {
                            end = index;
                        }
                    }
                    index = start;
//                    logger.info("Found index: " + index + " in " + (System.currentTimeMillis() - fileSearchStart) + " ms");
                }

                ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.MSG_BYTE_LENGTH * 100);
                int offset = index * Constants.MSG_BYTE_LENGTH;

                while (offset < size) {
                    int bytes = fileChannel.read(buffer, offset);
                    if (bytes <= 0) {
                        break;
                    }
                    offset += bytes;

                    buffer.flip();
                    boolean done = false;
                    while (buffer.remaining() > 0) {
                        long t = buffer.getLong();
                        if (t > tMax) {
                            done = true;
                            break;
                        }
                        long a = buffer.getLong();
                        if (a >= aMin && a <= aMax) {
                            sum += a;
                            count++;
                        }
                        buffer.position(buffer.position() + Constants.BODY_BYTE_LENGTH);
                    }
                    if (done) {
                        break;
                    }
                    buffer.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return count == 0 ? 0 : sum / count;
    }

    private void persistMemTable(NavigableMap<Long, List<Message>> frozenMemTable) {
        String fileName = Constants.DATA_DIR+ "sst" + persistCounter.getAndIncrement() + ".data";
        logger.info("Start persisting memTable to file: " + fileName);

        RandomAccessFile ssTableFile = null;
        try {
            ssTableFile = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException e) {
            logger.info("File not found", e);
            throw new RuntimeException(e);
        }
        FileChannel fileChannel = ssTableFile.getChannel();

        ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.MSG_BYTE_LENGTH * 100);
        int writeCount = 0;
        int writeBytes = 0;
        for (Map.Entry<Long, List<Message>> entry : frozenMemTable.entrySet()) {
            for (Message msg : entry.getValue()) {
                if (buffer.remaining() < Constants.MSG_BYTE_LENGTH) {
                    buffer.flip();
                    try {
                        writeBytes += buffer.limit();
                        fileChannel.write(buffer);
                    } catch (IOException e) {
                        logger.info("ERROR write to file channel: " + e.getMessage());
                    }
                    buffer.clear();
                }
                buffer.putLong(msg.getT());
                buffer.putLong(msg.getA());
                buffer.put(msg.getBody());
                writeCount++;
            }
        }

        buffer.flip();
        try {
            writeBytes += buffer.limit();
            fileChannel.write(buffer);
        } catch (IOException e) {
            logger.info("ERROR write to file channel: " + e.getMessage());
        }
        buffer.clear();

        ssTableFileList.add(new SSTableFile(ssTableFile, fileChannel,
                frozenMemTable.firstEntry().getKey(), frozenMemTable.lastEntry().getKey()));
        logger.info("Done persisting memTable to file: " + fileName
                + ", written msgs: " + writeCount + ", written bytes: " + writeBytes);
    }

    private static class SSTableFile {
        RandomAccessFile randomAccessFile;
        FileChannel fileChannel;
        long tStart = 0;
        long tEnd = 0;

        SSTableFile(RandomAccessFile randomAccessFile, FileChannel fileChannel, long tStart, long tEnd) {
            this.randomAccessFile = randomAccessFile;
            this.fileChannel = fileChannel;
            this.tStart = tStart;
            this.tEnd = tEnd;
        }
    }

//    private Long buildKey(Message message) {
//        return message.getA()
//    }

//    private static class MessageValue {
//        private long a;
//        private byte[] body;
//
//        public MessageValue(long a, byte[] body) {
//            this.a = a;
//            this.body = body;
//        }
//
//        public long getA() {
//            return a;
//        }
//
//        public void setA(long a) {
//            this.a = a;
//        }
//
//        public byte[] getBody() {
//            return body;
//        }
//
//        public void setBody(byte[] body) {
//            this.body = body;
//        }
//    }
}
