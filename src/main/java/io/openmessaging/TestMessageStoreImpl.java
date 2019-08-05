package io.openmessaging;


import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author .ignore 2019-07-29
 */
public class TestMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(TestMessageStoreImpl.class);

    private AtomicLong counter = new AtomicLong();
    private long _aMin = Long.MAX_VALUE;
    private long _aMax = Long.MIN_VALUE;
    private long _tMin = Long.MAX_VALUE;
    private long _tMax = Long.MIN_VALUE;

    @Override
    public synchronized void put(Message message) {
//        if (counter.incrementAndGet() < 100000) {
//            logger.info("t - " + message.getT() + ", a - " + message.getA() + ", body.length - " + message.getBody().length);
//        }
        counter.incrementAndGet();
        _aMin = Math.min(message.getA(), _aMin);
        _aMax = Math.max(message.getA(), _aMax);
        _tMin = Math.min(message.getT(), _tMin);
        _tMax = Math.max(message.getT(), _tMax);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        logger.info("getMessage: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
        logger.info("getMessage: _aMin - " + _aMin + ", _aMax - " + _aMax
                + ", _tMin - " + _tMin + ", _tMax - " + _tMax + ", counter = " + counter);

        ArrayList<Message> res = new ArrayList<Message>();
        return res;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        logger.info("getAvgValue: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
        long sum = 0;
        long count = 0;
        return count == 0 ? 0 : sum / count;
    }
}
