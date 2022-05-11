package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;

public class VoltProcedureContainer extends VoltFunction {

    public static final SQLStmt addResult = new SQLStmt(
            // KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public static final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue, KVKey FROM KVTable WHERE KVKey=?;"
    );
}
