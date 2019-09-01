package io.openmessaging;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author pengqun.pq
 */
class PerfStats {

    private static final Logger logger = Logger.getLogger(PerfStats.class);

    static final int PUT_SAMPLE_RATE = 100000000;
    static final int REWRITE_SAMPLE_RATE = 100000000;
    static final int GET_SAMPLE_RATE = 1000;
    static final int AVG_SAMPLE_RATE = 1000;

    static AtomicLong putMsgCounter = new AtomicLong();
    static AtomicLong getMsgCounter = new AtomicLong();
    static AtomicLong avgMsgCounter = new AtomicLong();

    static AtomicInteger readACounter  = new AtomicInteger();
    static AtomicInteger usedACounter  = new AtomicInteger();
    static AtomicInteger skipACounter = new AtomicInteger();
    static AtomicInteger readAICounter  = new AtomicInteger();
    static AtomicInteger usedAICounter  = new AtomicInteger();
    static AtomicInteger skipAICounter  = new AtomicInteger();
    static AtomicInteger jump1AICounter  = new AtomicInteger();
    static AtomicInteger jump2AICounter  = new AtomicInteger();
    static AtomicInteger jump3AICounter  = new AtomicInteger();
    static AtomicInteger fastSmallerCounter = new AtomicInteger();
    static AtomicInteger fastLargerCounter = new AtomicInteger();

    static long _putStart = 0;
    static long _putEnd = 0;
    static long _getStart = 0;
    static long _getEnd = 0;
    static long _avgStart = 0;

    static void printStats(DefaultMessageStoreImpl messageStore) {
        long putDuration = _putEnd - _putStart;
        long getDuration = _getEnd - _getStart;
        long avgDuration = System.currentTimeMillis() - _avgStart;
        int putScore = (int) (messageStore.getPutCounter().get() / putDuration);
        int getScore = (int) (getMsgCounter.get() / getDuration);
        int avgScore = (int) (avgMsgCounter.get() / avgDuration);
        int totalScore = putScore + getScore + avgScore;
        logger.info("Avg result: \n"
                + "\tread a: " + readACounter.get() + ", used a: " + usedACounter.get()
                + ", skip a: " + skipACounter.get()
                + ", use ratio: " + ((double) usedACounter.get() / readACounter.get())
                + ", visit ratio: " + ((double) (usedACounter.get() + skipACounter.get()) / readACounter.get()) + "\n"
                + "\tread ai: " + readAICounter.get() + ", used ai: " + usedAICounter.get() + ", skip ai: " + skipAICounter.get()
                + ", jump ai: " + jump1AICounter.get() + " / " + jump2AICounter.get() + " / " + jump3AICounter.get()
                + ", use ratio: " + ((double) usedAICounter.get() / readAICounter.get()) + "\n"
                + ", visit ratio: " + ((double) (usedAICounter.get() + skipAICounter.get()) / readAICounter.get()) + "\n"
                + "\tcover ratio: " + ((double) usedAICounter.get() / (usedAICounter.get() + usedACounter.get()))+ "\n"
                + "\tfast smaller: " + fastSmallerCounter.get() + ", larger: " + fastLargerCounter.get() + "\n"
        );
        logger.info("Test result: \n"
                + "\tput: " + messageStore.getPutCounter().get() + " / " + putDuration + "ms = " + putScore + "\n"
                + "\tget: " + getMsgCounter.get() + " / " + getDuration + "ms = " + getScore + "\n"
                + "\tavg: " + avgMsgCounter.get() + " / " + avgDuration + "ms = " + avgScore + "\n"
                + "\ttotal: " + totalScore + "\n"
        );
        throw new RuntimeException(putScore + "/" + getScore + "/" + avgScore);
    }
}
