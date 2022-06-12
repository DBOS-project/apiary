package org.dbos.apiary.function;

import java.util.List;

public class TransactionContext {

    public final long txID;
    public final long xmin;
    public final long xmax;
    public final List<Long> activeTransactions;

    public TransactionContext(long txID, long xmin, long xmax, List<Long> activeTransactions) {
        this.txID = txID;
        this.xmin = xmin;
        this.xmax = xmax;
        this.activeTransactions = activeTransactions;
    }
}
