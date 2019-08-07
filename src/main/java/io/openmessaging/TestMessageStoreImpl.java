package io.openmessaging;


import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

    private long minDiff = Long.MAX_VALUE;
    private long maxDiff = Long.MIN_VALUE;
    private long totalPosDiff = 0;
    private long posDiffCount = 0;
    private long totalNegDiff = 0;
    private long negDiffCount = 0;

//    private short[] msgCounter = new short[1100000000];
    private int minRepeat = Integer.MAX_VALUE;
    private int maxRepeat = Integer.MIN_VALUE;

    private int[] accumCounter = new int[800 * 1024 * 1024];
    private int minAccum = Integer.MAX_VALUE;
    private int maxAccum = Integer.MIN_VALUE;

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
    ByteBuffer byteBuffer2 = ByteBuffer.allocateDirect(1024 * 1024 * 1024);

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

        long diff = message.getT() - message.getA();
        minDiff = Math.min(diff, minDiff);
        maxDiff = Math.max(diff, maxDiff);
        if (diff > 0) {
            totalPosDiff += diff;
            posDiffCount++;
        } else if (diff < 0) {
            totalNegDiff += diff;
            negDiffCount++;
        }

//        msgCounter[(int) message.getT()]++;
//        minRepeat = Math.min(minRepeat, msgCounter[(int) message.getT()]);
//        maxRepeat = Math.max(maxRepeat, msgCounter[(int) message.getT()]);

        int t = (int) message.getT();
        if (t < accumCounter.length) {
            accumCounter[t] += diff;
            minAccum = Math.min(minAccum, accumCounter[t]);
            maxAccum = Math.max(maxAccum, accumCounter[t]);
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        logger.info("getMessage: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
        logger.info("getMessage: _aMin - " + _aMin + ", _aMax - " + _aMax
                + ", _tMin - " + _tMin + ", _tMax - " + _tMax + ", totalCount = " + totalCount
                + ", minDiff - " + minDiff + ", maxDiff - " + maxDiff
                + ", posDiffCount - " + posDiffCount + ", negDiffCount - " + negDiffCount
                + ", avgPosDiff - " + (posDiffCount > 0 ? totalPosDiff / posDiffCount : 0)
                + ", avgNegDiff - " + (negDiffCount > 0 ? totalNegDiff / negDiffCount : 0)
                + ", minRepeat - " + minRepeat + ", maxRepeat - " + maxRepeat
                + ", minAccum - " + minAccum + ", maxAccum - " + maxAccum
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
