package org.dbos.apiary.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

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

    public static byte[] stringArraytoByteArray(String[] strs) {
        int totalLen = 0;
        for (String s: strs) {
            totalLen += s.getBytes().length + 4;
        }
        byte[] bytes = new byte[totalLen];
        int i = 0;
        for (String str: strs) {
            int len = str.getBytes().length;
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(len);
            byte[] lenArray = bb.array();
            System.arraycopy(lenArray, 0, bytes, i, 4);
            byte[] strArray = str.getBytes();
            System.arraycopy(strArray, 0, bytes, i + 4, len);
            i += len + 4;
        }
        return bytes;
    }

    public static String[] byteArrayToStringArray(byte[] bytes) {
        ArrayList<String> strList = new ArrayList<>();
        for (int i = 0; i < bytes.length;) {
            byte[] lenArray = new byte[4];
            System.arraycopy(bytes, i, lenArray, 0, 4);
            ByteBuffer wrapped = ByteBuffer.wrap(lenArray);
            int len = wrapped.getInt();
            byte[] strArray = new byte[len];
            System.arraycopy(bytes, i + 4, strArray, 0, len);
            strList.add(new String(strArray));
            i += len + 4;
        }
        return strList.toArray(new String[0]);
    }

}
