package org.dbos.apiary.utilities;

public class ApiaryConfig {
    public static final int voltdbPort = 21212;
    public static final int workerPort = 8000;
    public static final int postgresPort = 5432;
    public static int provenancePort = 5433;  // Change this to the correct provenance DB port.
    public static final int mysqlPort = 3306;
    public static final int mongoPort = 27017;
    public static final long statelessTxid = 1L;
    public static final String tableFuncInvocations = "FUNCINVOCATIONS";

    public static Boolean captureUpdates = false;
    public static Boolean captureReads = false;
    public static Boolean captureMetadata = true;
    public static Boolean captureFuncInvocations = true;
    public static final String provenanceDefaultAddress = "localhost";

    public static final String defaultRole = "DefaultRole";
    public static final String systemRole = "ApiarySysAdmin";

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
    public static final String dbosDBName = "dbos";

    // GCS bucket names.
    public static final String gcsTestBucket = "apiary_gcs_test";
    public static final String mysql = "mysql";

    // Replay mode.
    public enum ReplayMode {
        NOT_REPLAY(0),
        SINGLE(1),
        ALL(2),
        SELECTIVE(3);

        private int value;

        private ReplayMode (int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static Boolean recordInput = false;  // If true, capture input of the entry function and record in a table.
    public static final String tableRecordedInputs = "RECORDEDINPUTS";

    public static Boolean trackCommitTimestamp = false;  // If true, Postgres is configured to track commit timestamp of each transaction.
}
