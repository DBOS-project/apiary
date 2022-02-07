package org.dbos.apiary.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Utilities {

    private static final Logger logger = LoggerFactory.getLogger(Utilities.class);

    public static byte[] objectToByteArray(Serializable obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Serialization Failed {} {}", obj, e);
            assert(false);
        }
        return bos.toByteArray();
    }

    public static Object byteArrayToObject(byte[] b) {
        ByteArrayInputStream bis = new ByteArrayInputStream(b);
        Object obj = null;
        try {
            ObjectInput in = new ObjectInputStream(bis);
            obj = in.readObject();
            in.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            logger.error("Deserialization Failed {} {}", b, e);
            assert(false);
        }
        return obj;
    }
}
