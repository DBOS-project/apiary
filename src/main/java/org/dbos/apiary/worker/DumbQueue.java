package org.dbos.apiary.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.PriorityBlockingQueue;

public class DumbQueue<E> extends PriorityBlockingQueue<E> {
    private static final Logger logger = LoggerFactory.getLogger(DumbQueue.class);
    public boolean offer(E e) {
        long t0 = System.nanoTime();
        boolean dumb = super.offer(e);
        logger.info("{}", System.nanoTime() - t0);
        return dumb;
    }
}
