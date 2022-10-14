package org.dbos.apiary.utilities;

public class ScopedTimer implements AutoCloseable {
    private long t0;
    private ElapsedTimeProcessor processor;
    public ScopedTimer(ElapsedTimeProcessor processor) {
        t0 = System.nanoTime();
        this.processor = processor;
    }

    @Override
    public void close() throws Exception {
        long interval = System.nanoTime() - t0;
        this.processor.operation(interval);
    }

}
