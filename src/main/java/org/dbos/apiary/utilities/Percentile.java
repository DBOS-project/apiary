package org.dbos.apiary.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Percentile {
    private List<Long> times = new ArrayList<>();
    private boolean isSorted = false;

    public Percentile() {}

    synchronized public void add(Long t) {
        isSorted = false;
        times.add(t);
    }

    synchronized public Long nth(int n) {
        if (times.size() == 0) {
            return Long.valueOf(0);
        }
        sort();
        assert(n > 0 && n <= 100);
        int sz = times.size();
        int i = (int)(Math.ceil(n / 100.0 * sz) - 1.0);
        assert(i >= 0 && i < times.size());
        return times.get(i);
    }

    private void sort() {
        if (isSorted) {
            return;
        }
        Collections.sort(times);
        isSorted = true;
    }

    synchronized public Long average() {
        long sum = times.stream().mapToLong(Long::longValue).sum();;
        return (long)((double)sum / (double)(times.size() + 0.001));
    }

    synchronized public int size() {
        return times.size();
    }
}
