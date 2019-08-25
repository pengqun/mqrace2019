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

    private int totalCount = 0;

    private long _aMin = Long.MAX_VALUE;
    private long _aMax = Long.MIN_VALUE;
    private long _tMin = Long.MAX_VALUE;
    private long _tMax = Long.MIN_VALUE;

    private long minDiff = Long.MAX_VALUE;
    private long maxDiff = Long.MIN_VALUE;
    private long totalPosDiff = 0;
    private long posDiffCount = 0;
    private long totalNegDiff = 0;
    private long negDiffCount = 0;

    private short[] msgCounter = new short[1200 * 1024 * 1024];
    private int minRepeat = Integer.MAX_VALUE;
    private int maxRepeat = Integer.MIN_VALUE;

//    private int[] accumCounter = new int[800 * 1024 * 1024];
    private int minAccum = Integer.MAX_VALUE;
    private int maxAccum = Integer.MIN_VALUE;

//    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//    ByteBuffer byteBuffer2 = ByteBuffer.allocateDirect(1024 * 1024 * 1024);

//    private long[] diffCounter = new long[65535];
    private int[] repeatCounter = new int[1024 * 1024];

    private AtomicInteger getCounter = new AtomicInteger(0);

    private long base = 0;

//    private List<byte[]> bodyBuffer = new ArrayList<>();

    @Override
    public synchronized void put(Message message) {
        long t = message.getT();
        long a = message.getA();
        byte[] body = message.getBody();

        StringBuilder sb = new StringBuilder();
        for (byte b : body) {
            sb.append(b).append("-");
        }
        System.out.println(sb);

        int id = totalCount++;
        if (totalCount == 128) {
            throw new RuntimeException("quit");
        }

        if (id == 0) {
            base = t - 10000;
        }
        while (base == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int tDiff = (int) (t - base);
        if (tDiff < 0) {
            throw new RuntimeException("Wrong base with diff: " + tDiff);
        }

        _aMin = Math.min(a, _aMin);
        _aMax = Math.max(a, _aMax);
        _tMin = Math.min(t, _tMin);
        _tMax = Math.max(t, _tMax);

//        long diff = a - t;
//        minDiff = Math.min(diff, minDiff);
//        maxDiff = Math.max(diff, maxDiff);
//        if (diff > 0) {
//            totalPosDiff += diff;
//            posDiffCount++;
//        } else if (diff < 0) {
//            totalNegDiff += diff;
//            negDiffCount++;
//        }
//        diffCounter[diff]++;

        msgCounter[tDiff]++;
        if (msgCounter[tDiff] < 0) {
            throw new RuntimeException("Repeat larger than short");
        }

        minRepeat = Math.min(minRepeat, msgCounter[tDiff]);
        maxRepeat = Math.max(maxRepeat, msgCounter[tDiff]);

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
        logger.info("getMessage: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
        for (long t = _tMin; t <= _tMax; t++) {
            repeatCounter[msgCounter[(int) (t - base)]]++;
        }

        int nonZeroCount = 0;
//        for (int diff = minDiff; diff <= maxDiff; diff++) {
//            logger.info(" diff: " + diff + " -> " + diffCounter[diff]);
//            if (diffCounter[diff] > 0) {
//                nonZeroCount++;
//            }
//        }
//        logger.info(" non-zero diff: " + nonZeroCount);

//        nonZeroCount = 0;
        for (int repeat = 0; repeat <= maxRepeat; repeat++) {
            logger.info(" repeat: " + repeat + " -> " + repeatCounter[repeat]);
            if (repeatCounter[repeat] > 0) {
                nonZeroCount++;
            }
        }
        logger.info(" non-zero repeat: " + nonZeroCount);

        logger.info("getMessage: _aMin - " + _aMin + ", _aMax - " + _aMax
                + ", _tMin - " + _tMin + ", _tMax - " + _tMax + ", totalCount = " + totalCount
//                + ", minDiff - " + minDiff + ", maxDiff - " + maxDiff
//                + ", posDiffCount - " + posDiffCount + ", negDiffCount - " + negDiffCount
//                + ", avgPosDiff - " + (posDiffCount > 0 ? totalPosDiff / posDiffCount : 0)
//                + ", avgNegDiff - " + (negDiffCount > 0 ? totalNegDiff / negDiffCount : 0)
                + ", minRepeat - " + minRepeat + ", maxRepeat - " + maxRepeat
//                + ", minAccum - " + minAccum + ", maxAccum - " + maxAccum
        );

        ArrayList<Message> res = new ArrayList<>();
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
