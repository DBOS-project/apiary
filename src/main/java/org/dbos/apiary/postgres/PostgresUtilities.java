package org.dbos.apiary.postgres;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    public static List<Long> parseActiveTransactions(String snapshotString) {
        String[] splitString = snapshotString.split(":");
        if (splitString.length == 3 ) {
            return Arrays.stream(snapshotString.split(":")[2].split(",")).map(Long::parseLong).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    public static String constuctSnapshotStr(long xmin, long xmax, List<Long> activeTxns) {
        List<String> activeStr = activeTxns.stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
        return String.format("%d:%d:%s", xmin, xmax, String.join(",", activeStr));
    }
}
