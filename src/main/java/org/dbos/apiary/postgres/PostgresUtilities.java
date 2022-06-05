package org.dbos.apiary.postgres;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class PostgresUtilities {

    public static void prettyPrintResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }

    public static long parseXmin(String snapshotString) {
        return Long.parseLong(snapshotString.split(":")[0]);
    }

    public static long parseXmax(String snapshotString) {
        return Long.parseLong(snapshotString.split(":")[1]);
    }

    public static long[] parseActiveTransactions(String snapshotString) {
        String[] splitString = snapshotString.split(":");
        if (splitString.length == 3 ) {
            return Arrays.stream(snapshotString.split(":")[2].split(",")).mapToLong(Long::parseLong).toArray();
        } else {
            return new long[0];
        }
    }
}
