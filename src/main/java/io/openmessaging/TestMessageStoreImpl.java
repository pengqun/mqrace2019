package io.openmessaging;


import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author .ignore 2019-07-29
 */
public class TestMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(TestMessageStoreImpl.class);

    private long totalCount = 0;
    private long _aMin = Long.MAX_VALUE;
    private long _aMax = Long.MIN_VALUE;
    private long _tMin = Long.MAX_VALUE;
    private long _tMax = Long.MIN_VALUE;

    private int minDiff = Integer.MAX_VALUE;
    private int maxDiff = Integer.MIN_VALUE;
    private long totalPosDiff = 0;
    private long posDiffCount = 0;
    private long totalNegDiff = 0;
    private long negDiffCount = 0;

    private short[] msgCounter = new short[1024 * 1024 * 1024];
    private int minRepeat = Integer.MAX_VALUE;
    private int maxRepeat = Integer.MIN_VALUE;

//    private int[] accumCounter = new int[800 * 1024 * 1024];
    private int minAccum = Integer.MAX_VALUE;
    private int maxAccum = Integer.MIN_VALUE;

//    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//    ByteBuffer byteBuffer2 = ByteBuffer.allocateDirect(1024 * 1024 * 1024);

    private int[] diffCounter = new int[65535];
    private int[] repeatCounter = new int[5000];

    private AtomicInteger getCounter = new AtomicInteger(0);

    @Override
    public synchronized void put(Message message) {
//        if (counter.incrementAndGet() < 100000) {
//            logger.info("t - " + message.getT() + ", a - " + message.getA() + ", body.length - " + message.getBody().length);
//        }
        totalCount++;
        _aMin = Math.min(message.getA(), _aMin);
        _aMax = Math.max(message.getA(), _aMax);
        _tMin = Math.min(message.getT(), _tMin);
        _tMax = Math.max(message.getT(), _tMax);

        int diff = (int) (message.getA() - message.getT()) + 10000;
        minDiff = Math.min(diff, minDiff);
        maxDiff = Math.max(diff, maxDiff);
//        if (diff > 0) {
//            totalPosDiff += diff;
//            posDiffCount++;
//        } else if (diff < 0) {
//            totalNegDiff += diff;
//            negDiffCount++;
//        }

        diffCounter[diff]++;

        int t = (int) message.getT();
        msgCounter[t]++;
        minRepeat = Math.min(minRepeat, msgCounter[t]);
        maxRepeat = Math.max(maxRepeat, msgCounter[t]);

//        if (t < accumCounter.length) {
//            accumCounter[t] += diff;
//            minAccum = Math.min(minAccum, accumCounter[t]);
//            maxAccum = Math.max(maxAccum, accumCounter[t]);
//        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (getCounter.getAndIncrement() > 0) {
            try {
                Thread.sleep(1000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        logger.info("getMessage: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
        for (int t = (int) _tMin; t <= _tMax; t++) {
            repeatCounter[msgCounter[t]]++;
        }

        for (int diff = minDiff; diff <= maxDiff; diff++) {
            logger.info(" diff: " + diff + " -> " + diffCounter[diff]);
        }
        for (int repeat = 0; repeat <= maxRepeat; repeat++) {
            logger.info(" repeat: " + repeat + " -> " + repeatCounter[repeat]);
        }

        logger.info("getMessage: _aMin - " + _aMin + ", _aMax - " + _aMax
                + ", _tMin - " + _tMin + ", _tMax - " + _tMax + ", totalCount = " + totalCount
//                + ", minDiff - " + minDiff + ", maxDiff - " + maxDiff
//                + ", posDiffCount - " + posDiffCount + ", negDiffCount - " + negDiffCount
//                + ", avgPosDiff - " + (posDiffCount > 0 ? totalPosDiff / posDiffCount : 0)
//                + ", avgNegDiff - " + (negDiffCount > 0 ? totalNegDiff / negDiffCount : 0)
//                + ", minRepeat - " + minRepeat + ", maxRepeat - " + maxRepeat
//                + ", minAccum - " + minAccum + ", maxAccum - " + maxAccum
        );

        ArrayList<Message> res = new ArrayList<Message>();
        return res;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        logger.info("getAvgValue: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
        long sum = 0;
        long count = 0;
        return count == 0 ? 0 : sum / count;
    }
}
