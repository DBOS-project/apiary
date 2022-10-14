package org.dbos.apiary.utilities;

import java.util.concurrent.Callable;

public interface ElapsedTimeProcessor {
    void operation(long elapsedNanos);
}
