package org.dbos.apiary.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DumbQueue<E> extends PriorityQueue<E> implements BlockingQueue<E> {
    private static final Logger logger = LoggerFactory.getLogger(DumbQueue.class);

    Lock lock = new ReentrantLock();
    Condition notEmpty = lock.newCondition();

    public DumbQueue() {
        super();
    }

    @Override
    public boolean offer(E e) {
        long t0 = System.nanoTime();
        lock.lock();
        long t1 = System.nanoTime() - t0;
        boolean b;
        try {
            b = super.offer(e);
            this.notEmpty.signal();
        } finally {
            lock.unlock();
        }
        long t2 = System.nanoTime() - t0;
        logger.info("{} {}", t1, t2);
        return b;
    }

    @Override
    public E take() throws InterruptedException {
        lock.lockInterruptibly();

        E result;
        try {
            while ((result = super.poll()) == null) {
                this.notEmpty.await();
            }
        } finally {
            lock.unlock();
        }

        return result;
    }

    /** IGNORE **/

    @Override
    public void put(E e) throws InterruptedException {
        logger.error("NO DON'T CALL ME");
        assert(false);
    }

    @Override
    public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
        logger.error("NO DON'T CALL ME");
        assert(false);
        return false;
    }

    @Override
    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        logger.error("NO DON'T CALL ME");
        assert(false);
        return null;
    }

    @Override
    public int remainingCapacity() {
        logger.error("NO DON'T CALL ME");
        assert(false);
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> collection) {
        logger.error("NO DON'T CALL ME");
        assert(false);
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> collection, int i) {
        logger.error("NO DON'T CALL ME");
        assert(false);
        return 0;
    }
}
