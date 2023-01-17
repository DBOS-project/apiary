package org.dbos.apiary.function;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionContext {

    public final long txID;
    public final long xmin;
    public final long xmax;
    public final List<Long> activeTransactions;
    public final AtomicInteger querySeqNum;  // Query sequence number within a transaction, starting from 0.
    public boolean readOnly = true;  // If true, transaction is read-only.

    public TransactionContext(long txID, long xmin, long xmax, List<Long> activeTransactions) {
        this.txID = txID;
        this.xmin = xmin;
        this.xmax = xmax;
        this.activeTransactions = activeTransactions;
        this.querySeqNum = new AtomicInteger(0);
    }
}
