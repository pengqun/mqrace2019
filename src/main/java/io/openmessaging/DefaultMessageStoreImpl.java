package io.openmessaging;

import java.util.List;

/**
 * Entry
 */
public class DefaultMessageStoreImpl extends MessageStore {

//    private MessageStore myMessageStore = new SampleMessageStoreImpl();
//    private MessageStore myMessageStore = new TestMessageStoreImpl();
//    private MessageStore myMessageStore = new LsmMessageStoreImpl();
    private MessageStore myMessageStore = new NewMessageStoreImpl();

    @Override
    public void put(Message message) {
        myMessageStore.put(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        return myMessageStore.getMessage(aMin, aMax, tMin, tMax);
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return myMessageStore.getAvgValue(aMin, aMax, tMin, tMax);
    }
}
