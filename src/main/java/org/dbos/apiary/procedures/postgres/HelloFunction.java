package org.dbos.apiary.procedures.postgres;

import org.postgresql.pljava.annotation.Function;

import java.sql.*;

public class HelloFunction {
    private static String m_url = "jdbc:default:connection";

    @Function
    public static String greet(String personName) throws SQLException {
        Connection conn = DriverManager.getConnection( m_url );
        String query = "SELECT * FROM KVTable WHERE KVKey = ?";
        PreparedStatement stmt = conn.prepareStatement( query );
        stmt.setInt(1, 0);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int val = rs.getInt("KVvalue");
        stmt.close();
        conn.close();

        return "Hello World, " + personName + " " + val + " !";
    }
}
