package org.dbos.apiary.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Percentile {
    private ConcurrentLinkedQueue<Long> times = new ConcurrentLinkedQueue<>();
    private ArrayList<Long> timesArray = null;
    private volatile boolean isSorted = false;

    public Percentile() {}

    public void add(Long t) {
        times.add(t);
        isSorted = false;
    }

    synchronized public Long nth(int n) {
        sort();
        if (timesArray.size() == 0) {
            return Long.valueOf(0);
        }
        assert(n > 0 && n <= 100);
        int sz = timesArray.size();
        int i = (int)(Math.ceil(n / 100.0 * sz) - 1.0);
        assert(i >= 0 && i < timesArray.size());
        return timesArray.get(i);
    }

    private void sort() {
        if (isSorted) {
            return;
        }
        timesArray = new ArrayList<Long>(times);
        Collections.sort(timesArray);
        isSorted = true;
    }

    synchronized public Long average() {
        long sum = times.stream().mapToLong(Long::longValue).sum();;
        return (long)((double)sum / (double)(times.size() + 0.001));
    }

    synchronized public int size() {
        return times.size();
    }

    synchronized public void clear() {
        times.clear();
        isSorted = false;
    }
}
