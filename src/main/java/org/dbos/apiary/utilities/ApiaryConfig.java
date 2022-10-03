package org.dbos.apiary.utilities;

public class ApiaryConfig {
    public static final int voltdbPort = 21212;
    public static final int workerPort = 8000;
    public static final int postgresPort = 5432;
    public static final int mysqlPort = 3306;
    public static final int mongoPort = 27017;
    public static final long statelessTxid = 1L;
    public static final String tableFuncInvocations = "FUNCINVOCATIONS";

    public static Boolean captureUpdates = true;
    public static Boolean captureReads = true;
    public static final String provenanceDefaultAddress = "localhost";

    public static final int REPEATABLE_READ = 1;
    public static final int SERIALIZABLE = 2;

    public static int isolationLevel = REPEATABLE_READ;

    public static boolean XDBTransactions = true;
    public static final Boolean profile = Boolean.FALSE;

    // For system functions.
    public static final String getApiaryClientID = "GetApiaryClientID";

    // Database names.
    public static final String stateless = "stateless";
    public static final String elasticsearch = "elasticsearch";
    public static final String postgres = "postgres";
    public static final String voltdb = "voltdb";
    public static final String vertica = "vertica";
    public static final String mongo = "mongo";
    public static final String gcs = "gcs";

    // GCS bucket names.
    public static final String gcsTestBucket = "apiary_gcs_test";
    public static final String mysql = "mysql";
}
