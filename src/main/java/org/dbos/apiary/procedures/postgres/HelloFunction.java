package org.dbos.apiary.procedures.postgres;

import org.dbos.apiary.postgres.PostgresFunctionInterface;
import org.postgresql.pljava.annotation.Function;

import java.sql.*;

public class HelloFunction {

    @Function
    public static String greet(String personName) throws SQLException {
        PostgresFunctionInterface p = new PostgresFunctionInterface(HelloFunction.class);
        return p.runFunction(personName).stringOutput;
    }

    public static String runSP(String personName) throws SQLException {
        String m_url = "jdbc:default:connection";
        Connection conn = DriverManager.getConnection(m_url);
        String query = "SELECT * FROM KVTable WHERE KVKey = ?";
        PreparedStatement stmt = conn.prepareStatement( query );
        stmt.setInt(1, 0);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int val = rs.getInt("KVvalue");
        stmt.close();
        conn.close();
        return "Hello World, " + personName + " " + val + "!";
    }
}
