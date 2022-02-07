package org.dbos.apiary;

import org.dbos.apiary.utilities.Utilities;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldTest {
    private static final Logger logger = LoggerFactory.getLogger(HelloWorldTest.class);

    @Test
    public void testHelloWorld() {
        logger.info("Hello, world!");
        String helloWorld = "Hello, world!";
        helloWorld = (String) Utilities.byteArrayToObject(Utilities.objectToByteArray(helloWorld));
        logger.info("{}", helloWorld);
    }
}
