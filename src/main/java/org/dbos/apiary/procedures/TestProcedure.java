package org.dbos.apiary.procedures;

import org.dbos.apiary.interposition.ApiaryProcedure;
import org.voltdb.SQLStmt;

public class TestProcedure extends ApiaryProcedure {
    public final SQLStmt getValue = new SQLStmt (
            "SELECT IncrementValue FROM IncrementTable WHERE IncrementKey=?;"
    );

    public final SQLStmt updateValue = new SQLStmt (
            "UPSERT INTO IncrementTable VALUES (?, ?, ?);"
    );


    public String runFunction(String jsonInput) {
        return null;
    }
}
