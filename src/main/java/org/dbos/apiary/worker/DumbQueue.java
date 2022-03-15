package org.dbos.apiary.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DumbQueue<E> extends PriorityQueue<E> implements BlockingQueue<E> {
    private static final Logger logger = LoggerFactory.getLogger(DumbQueue.class);
    AtomicBoolean used = new AtomicBoolean(false);
    Semaphore semaphore = new Semaphore(0);

    public DumbQueue() {
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
        while (!used.compareAndSet(false, true));
        E result = super.poll();
        used.set(false);
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
