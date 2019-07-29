package io.openmessaging;


import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author .ignore 2019-07-29
 */
public class NaiveMessageStoreImpl extends MessageStore {

    private static final Logger logger = Logger.getLogger(NaiveMessageStoreImpl.class);
    private AtomicLong counter = new AtomicLong();

    @Override
    public void put(Message message) {
        if (counter.incrementAndGet() < 100000) {
            logger.info("t - " + message.getT() + ", a - " + message.getA() + ", body.length - " + message.getBody().length);
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        logger.info("getMessage: aMin - " + aMin + ", aMax - " + aMax + ", tMin - " + tMin + ", tMax - " + tMax);
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
