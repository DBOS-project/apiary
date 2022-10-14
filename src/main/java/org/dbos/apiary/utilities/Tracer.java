package org.dbos.apiary.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Tracer {
    public class Stat {
        public AtomicLong totalTime = new AtomicLong(0);

        public AtomicLong XDSTInitializationNanos = new AtomicLong(0);
        public AtomicLong XDSTExecutionNanos = new AtomicLong(0);
        public AtomicLong XDSTValidationNanos = new AtomicLong(0);
        public AtomicLong XDSTExecutionOverheadNanos = new AtomicLong(0);
        public AtomicLong XDSTCommitNanos = new AtomicLong(0);

        public AtomicLong XAInitializationNanos = new AtomicLong(0);
        public AtomicLong XAExecutionNanos = new AtomicLong(0);
        public AtomicLong XACommitPrepareNanos = new AtomicLong(0);
        public AtomicLong XACommitNanos = new AtomicLong(0);
    }

    ConcurrentHashMap<Long, Stat> stats = new ConcurrentHashMap<>(); // mapping of execId to stats
    ConcurrentHashMap<String, ConcurrentSkipListSet<Long>> validIds = new ConcurrentHashMap<>(); // mapping of execId to stats

    private Stat getStat(long id) {
        Stat s = stats.get(id);
        if (s == null) {
            stats.putIfAbsent(id, new Stat());
            return stats.get(id);
        } else {
            return s;
        }
    }

    private ConcurrentSkipListSet<Long> getValidIds(String category) {
        ConcurrentSkipListSet<Long> s = validIds.get(category);
        if (s == null) {
            validIds.putIfAbsent(category, new ConcurrentSkipListSet<Long>());
            return validIds.get(category);
        } else {
            return s;
        }
    }
    public void setTotalTime(long id, long t) {
        Stat s = getStat(id);
        s.totalTime.set(t);
    }

    public void setXDSTInitNanos(long id, long t) {
        Stat s = getStat(id);
        s.XDSTInitializationNanos.set(t);
    }

    public void setXDSTValidationNanos(long id, long t) {
        Stat s = getStat(id);
        s.XDSTValidationNanos.set(t);
    }

    public void setXDSTCommitNanos(long id, long t) {
        Stat s = getStat(id);
        s.XDSTCommitNanos.set(t);
    }


    public void setXDSTExecutionNanos(long id, long t) {
        Stat s = getStat(id);
        s.XDSTExecutionNanos.set(t);
    }

    public void addXDSTExecutionOverheadNanos(long id, long t) {
        Stat s = getStat(id);
        s.XDSTExecutionOverheadNanos.addAndGet(t);
    }


    public void setXAInitNanos(long id, long t) {
        Stat s = getStat(id);
        s.XAInitializationNanos.set(t);
    }

    public void setXAExecutionNanos(long id, long t) {
        Stat s = getStat(id);
        s.XAExecutionNanos.set(t);
    }

    public void setXACommitNanos(long id, long t) {
        Stat s = getStat(id);
        s.XACommitNanos.set(t);
    }

    public void setXACommitPrepareNanos(long id, long t) {
        Stat s = getStat(id);
        s.XACommitPrepareNanos.set(t);
    }

    public void addCategoryValidId(String category, long id) {
        getValidIds(category).add(id);
    }

    public List<Stat> orderStats(String category) {
        ConcurrentSkipListSet<Long> vids = getValidIds(category);
        ArrayList<Stat> lstStats = new ArrayList();
        for (Entry<Long, Stat> e: stats.entrySet()) {
            if (vids.contains(e.getKey())) {
                lstStats.add(e.getValue());
            }
        }

        lstStats.sort((p1, p2) -> (int)(p1.totalTime.get() - p2.totalTime.get()));
        System.out.printf("%s stats.size() %d validIds.size() %d lstStats.size() %d\n", category, stats.size(), validIds.size(), lstStats.size());
        return lstStats;
    }

    public void clear() {
        stats.clear();
        validIds.clear();
    }
}
