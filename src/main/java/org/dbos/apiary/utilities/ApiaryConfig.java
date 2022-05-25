package org.dbos.apiary.utilities;

public class ApiaryConfig {
    public static final int voltdbPort = 21212;
    public static final int cockroachdbPort = 26257;
    public static final int workerPort = 8000;
    public static final int postgresPort = 5432;
    public static final long statelessTxid = 1l;
    public static final String tableFuncInvocations = "FUNCINVOCATIONS";

    public static final Boolean captureUpdates = true;
    public static final Boolean captureReads = true;
    public static final String provenanceDefaultAddress = "localhost";

    // For system functions.
    public static final String getApiaryClientID = "GetApiaryClientID";

    // Database names;
    public static final String stateless = "stateless";
    public static final String elasticsearch = "elasticsearch";
    public static final String postgres = "postgres";
    public static final String voltdb = "voltdb";
    public static final String vertica = "vertica";
}
