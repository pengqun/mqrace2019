package io.openmessaging;

import java.util.*;

/**
 * @author .ignore 2019-07-29
 */
public class SampleMessageStoreImpl extends MessageStore {

    private NavigableMap<Long, List<Message>> msgMap = new TreeMap<Long, List<Message>>();

    @Override
    public synchronized void put(Message message) {
        if (!msgMap.containsKey(message.getT())) {
            msgMap.put(message.getT(), new ArrayList<Message>());
        }

        msgMap.get(message.getT()).add(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        ArrayList<Message> res = new ArrayList<Message>();
        NavigableMap<Long, List<Message>> subMap = msgMap.subMap(tMin, true, tMax, true);
        for (Map.Entry<Long, List<Message>> mapEntry : subMap.entrySet()) {
            List<Message> msgQueue = mapEntry.getValue();
            for (Message msg : msgQueue) {
                if (msg.getA() >= aMin && msg.getA() <= aMax) {
                    res.add(msg);
                }
            }
        }

        return res;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        long count = 0;
        NavigableMap<Long, List<Message>> subMap = msgMap.subMap(tMin, true, tMax, true);
        for (Map.Entry<Long, List<Message>> mapEntry : subMap.entrySet()) {
            List<Message> msgQueue = mapEntry.getValue();
            for (Message msg : msgQueue) {
                if (msg.getA() >= aMin && msg.getA() <= aMax) {
                    sum += msg.getA();
                    count++;
                }
            }
        }

        return count == 0 ? 0 : sum / count;
    }
}
