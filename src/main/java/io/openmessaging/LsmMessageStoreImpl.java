package io.openmessaging;


import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author .ignore 2019-07-29
 */
public class LsmMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(LsmMessageStoreImpl.class);

    private static final int MAX_MEM_TABLE_SIZE = 1000000;

    private static final int WRITE_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1000;
    private static final int READ_BUFFER_SIZE = Constants.MSG_BYTE_LENGTH * 1000;

    private static final int PERSIST_SAMPLE_RATE = 100;
    private static final int GET_SAMPLE_RATE = 1000;
    private static final int AVG_SAMPLE_RATE = 1000;

    static {
        logger.info("LsmMessageStoreImpl loaded");
    }

//    private NavigableMap<Long, List<Message>> memTable = new TreeMap<>();
//    private volatile NavigableMap<Long, List<Message>> memTable = new ConcurrentSkipListMap<>();
    private volatile NavigableMap<Long, Message> memTable = new ConcurrentSkipListMap<>();

    private static ThreadPoolExecutor persistThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private AtomicInteger putCounter = new AtomicInteger(0);
    private AtomicInteger fileCounter = new AtomicInteger(0);
    private volatile boolean persistDone = false;

    private AtomicInteger getCounter = new AtomicInteger(0);

    private AtomicInteger avgCounter = new AtomicInteger(0);

    private List<SSTableFile> ssTableFileList = new ArrayList<>();

    @Override
    public void put(Message message) {
        long key = (message.getT() << 32) + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        Message conflictMessage = memTable.put(key, message);
        if (conflictMessage != null) {
            logger.info("[WARN] Put conflict message back");
            put(conflictMessage);
        }

        if (putCounter.incrementAndGet() % MAX_MEM_TABLE_SIZE == 0) {
//            logger.info("Submit memTable persist task");
            NavigableMap<Long, Message> frozenMemTable = memTable;
            memTable = new ConcurrentSkipListMap<>();
            persistThreadPool.execute(() -> {
                persistMemTable(frozenMemTable);
            });
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        int getId = getCounter.getAndIncrement();
        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("getMessage - tMin: " + tMin + ", tMax: " + tMax
                    + ", aMin: " + aMin + ", aMax: " + aMax + ", getId: " + getId);
        }
        if (getId == 0) {
            logger.info("Flush all memTables before getMessage");
            while (persistThreadPool.getActiveCount() + persistThreadPool.getQueue().size() > 0) {
                logger.info("Waiting for previous persist tasks to finish");
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
//            persistThreadPool.shutdown();
            printSSTableList();
        }
        while (!persistDone) {
            logger.info("Waiting for all persist tasks to finish");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long getStart = System.currentTimeMillis();
        List<SSTableFile> targetFileList = new ArrayList<>();

        for (SSTableFile file : ssTableFileList) {
            if (tMax >= file.tStart && tMin <= file.tEnd) {
                targetFileList.add(file);
            }
        }
        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("Found target files list: " + targetFileList.stream().map(s -> s.id).collect(Collectors.toList())
                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
        }

        ArrayList<Message> result = new ArrayList<>();
        ByteBuffer bufferForT = ByteBuffer.allocateDirect(Constants.KEY_BYTE_LENGTH);
        ByteBuffer bufferForMsg = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);

        for (SSTableFile file : targetFileList) {
            FileChannel fileChannel = file.channel;
            try {
                int index = 0;
                if (tMin > file.tStart) {
                    int start = 0;
                    int end = file.msgs - 1;
                    while (start < end) {
                        index = (start + end) / 2;
                        fileChannel.read(bufferForT, index * Constants.MSG_BYTE_LENGTH);
                        bufferForT.flip();
                        long t = bufferForT.getInt();
                        bufferForT.clear();
                        if (t < tMin) {
                            start = index + 1;
                        } else {
                            end = index;
                        }
                    }
                    index = start;
                }
                if (getId % GET_SAMPLE_RATE == 0) {
                    logger.info("Found index: " + index + " for file: " + file.id
                            + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
                }

                int offset = index * Constants.MSG_BYTE_LENGTH;
                boolean allFound = false;

                while (!allFound && offset < file.size) {
                    offset += fileChannel.read(bufferForMsg, offset);
                    bufferForMsg.flip();

                    while (bufferForMsg.remaining() > 0) {
                        long t = bufferForMsg.getInt();
                        if (t > tMax) {
                            allFound = true;
                            break;
                        }
                        long a = bufferForMsg.getInt();
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

        result.sort((o1, o2) -> (int) (o1.getT() - o2.getT()));
//        printGetResult(result);

        if (getId % GET_SAMPLE_RATE == 0) {
            logger.info("Return sorted result with size: " + result.size()
                    + ", time: " + (System.currentTimeMillis() - getStart) + ", getId: " + getId);
        }
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        int avgId = avgCounter.getAndIncrement();
        if (avgId % AVG_SAMPLE_RATE == 0) {
            logger.info("getAvgValue - tMin: " + tMin + ", tMax: " + tMax
                    + ", aMin: " + aMin + ", aMax: " + aMax + ", getId: " + avgId);
        }
        long sum = 0;
        long count = 0;

        List<SSTableFile> targetFileList = new ArrayList<>();
        for (SSTableFile file : ssTableFileList) {
            if (tMax >= file.tStart && tMin <= file.tEnd) {
                targetFileList.add(file);
            }
        }

        ByteBuffer bufferForT = ByteBuffer.allocateDirect(Constants.KEY_BYTE_LENGTH);
        ByteBuffer bufferForMsg = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);

        for (SSTableFile file : targetFileList) {
            FileChannel fileChannel = file.channel;
            try {
                int index = 0;
                if (tMin > file.tStart) {
                    int start = 0;
                    int end = file.msgs - 1;
                    while (start < end) {
                        index = (start + end) / 2;
                        fileChannel.read(bufferForT, index * Constants.MSG_BYTE_LENGTH);
                        bufferForT.flip();
                        long t = bufferForT.getInt();
                        bufferForT.clear();
                        if (t < tMin) {
                            start = index + 1;
                        } else {
                            end = index;
                        }
                    }
                    index = start;
                }

                int offset = index * Constants.MSG_BYTE_LENGTH;
                boolean allFound = false;

                while (!allFound && offset < file.size) {
                    offset += fileChannel.read(bufferForMsg, offset);
                    bufferForMsg.flip();

                    while (bufferForMsg.remaining() > 0) {
                        long t = bufferForMsg.getInt();
                        if (t > tMax) {
                            allFound = true;
                            break;
                        }
                        long a = bufferForMsg.getInt();
                        if (a >= aMin && a <= aMax) {
                            sum += a;
                            count++;
                        }
                        bufferForMsg.position(bufferForMsg.position() + Constants.BODY_BYTE_LENGTH);
                    }
                    bufferForMsg.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return count == 0 ? 0 : sum / count;
    }

    private void persistMemTable(NavigableMap<Long, Message> frozenMemTable) {
        int fileId = fileCounter.getAndIncrement();
        String fileName = Constants.DATA_DIR+ "sst" + fileId + ".data";
        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Start persisting memTable to file: " + fileName);
        }

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException e) {
            logger.info("[ERROR] File not found", e);
            throw new RuntimeException(e);
        }

        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
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
            buffer.putInt((int) msg.getT());
            buffer.putInt((int) msg.getA());
            buffer.put(msg.getBody());
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

        ssTableFileList.add(new SSTableFile(fileId, raf, channel, frozenMemTable.firstEntry().getValue().getT(),
                frozenMemTable.lastEntry().getValue().getT()));

        if (fileId % PERSIST_SAMPLE_RATE == 0) {
            logger.info("Done persisting memTable to file: " + fileName
                    + ", written msgs: " + writeCount + ", written bytes: " + writeBytes);
        }
    }

    private static class SSTableFile {
        RandomAccessFile file;
        FileChannel channel;
        int id = -1;
        long tStart = 0;
        long tEnd = 0;
        int size = 0;
        int msgs = 0;

        SSTableFile(int id, RandomAccessFile file, FileChannel channel, long tStart, long tEnd) {
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
        }
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
