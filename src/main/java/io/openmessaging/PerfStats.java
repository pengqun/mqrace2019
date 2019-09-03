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

    static AtomicLong getMsgCounter = new AtomicLong();
    static AtomicLong avgMsgCounter = new AtomicLong();

    static long _putStart = 0;
    static long _putEnd = 0;
    static long _getStart = 0;
    static long _getEnd = 0;
    static long _avgStart = 0;

    static AtomicInteger cacheHit = new AtomicInteger();
    static AtomicInteger cacheMiss = new AtomicInteger();

    static void printStats(DefaultMessageStoreImpl messageStore) {
        long putDuration = _putEnd - _putStart;
        long getDuration = _getEnd - _getStart;
        long avgDuration = System.currentTimeMillis() - _avgStart;
        int putScore = (int) (messageStore.getPutCounter().get() / putDuration);
        int getScore = (int) (getMsgCounter.get() / getDuration);
        int avgScore = (int) (avgMsgCounter.get() / avgDuration);
        int totalScore = putScore + getScore + avgScore;

        logger.info("Cache result: \n"
                + "\thit: " + cacheHit + ", " + ", miss: " + cacheMiss + "\n"
                + "\tratio: " + (double) cacheHit.get() / (cacheHit.get() + cacheMiss.get()) + "\n"
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
