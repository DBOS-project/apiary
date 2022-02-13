package org.dbos.apiary.procedures.postgres;

import org.postgresql.pljava.annotation.Function;

public class HelloFunction {
    @Function
    public static String greet(String personName) {
        return "Hello World, " + personName + " !";
    }
}
