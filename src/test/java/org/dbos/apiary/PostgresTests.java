package org.dbos.apiary;

import org.dbos.apiary.postgres.PostgresConnection;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresTests.class);

    @Test
    public void testPostgres() throws Exception {
        logger.info("testPostgres");
        String url = "jdbc:postgresql://localhost/";
        Connection c = DriverManager.getConnection(url, "postgres", "password");
        PostgresConnection p = new PostgresConnection(c);
        String o = p.callFunction("greet", 0, "matei").stringOutput;
        assertEquals("Hello World, matei 1!", o);
    }
}
