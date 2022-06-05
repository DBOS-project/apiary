package org.dbos.apiary.function;

public class TransactionContext {

    public final long txID;
    public final long xmin;
    public final long xmax;
    public final long[] activeTransactions;

    public TransactionContext(long txID, long xmin, long xmax, long[] activeTransactions) {
        this.txID = txID;
        this.xmin = xmin;
        this.xmax = xmax;
        this.activeTransactions = activeTransactions;
    }
}
