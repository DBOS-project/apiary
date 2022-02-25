package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;

public class VoltProcedureContainer extends VoltApiaryProcedure {

    public static final SQLStmt addResult = new SQLStmt(
            // PKEY, KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?, ?);"
    );

    public static final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue FROM KVTable WHERE KVKey=?;"
    );
}
