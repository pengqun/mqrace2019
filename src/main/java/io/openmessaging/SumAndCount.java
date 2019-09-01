package io.openmessaging;

/**
 * @author pengqun.pq
 */
class SumAndCount {
    private long sum;
    private long count;

    SumAndCount(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    long getSum() {
        return sum;
    }

    long getCount() {
        return count;
    }
}
