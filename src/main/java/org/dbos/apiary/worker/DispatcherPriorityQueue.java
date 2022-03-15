package org.dbos.apiary.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DispatcherPriorityQueue<E> extends PriorityQueue<E> implements BlockingQueue<E> {
    private static final Logger logger = LoggerFactory.getLogger(DispatcherPriorityQueue.class);
    Lock lock = new ReentrantLock();
    AtomicBoolean used = new AtomicBoolean(false);
    Semaphore semaphore = new Semaphore(0);

    public DispatcherPriorityQueue() {
        super();
    }

    @Override
    public boolean offer(E e) {
        assert (e != null);
        boolean b;
        while (!used.compareAndSet(false, true));
        b = super.offer(e);
        used.set(false);
        semaphore.release();
        return b;
    }

    @Override
    public E take() throws InterruptedException {
        semaphore.acquire();
        lock.lock();
        while (!used.compareAndSet(false, true));
        E result = super.poll();
        used.set(false);
        lock.unlock();
        assert result != null;
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
