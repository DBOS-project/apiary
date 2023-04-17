package org.dbos.apiary.utilities;

import com.google.protobuf.ByteString;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utilities {
    private static final Logger logger = LoggerFactory.getLogger(Utilities.class);
    public static int stringType = 1;
    public static int stringArrayType = 2;
    public static int intType = 3;
    public static int intArrayType = 4;

    public static byte[] intArrayToByteArray(int[] ints) {
        byte[] bytes = new byte[ints.length * 4];
        for (int i = 0; i < ints.length; i++) {
            int value = ints[i];
            int bi = i * 4;
            bytes[bi] = (byte) (value >> 24);
            bytes[bi + 1] = (byte) (value >> 16);
            bytes[bi + 2] = (byte) (value >> 8);
            bytes[bi + 3] = (byte) (value);
        }
        return bytes;
    }

    public static byte[] longArrayToByteArray(long[] longs) {
        byte[] bytes = new byte[longs.length * 8];
        for (int i = 0; i < longs.length; i++) {
            long value = longs[i];
            int bi = i * 8;
            bytes[bi] = (byte) (value >> 56);
            bytes[bi + 1] = (byte) (value >> 48);
            bytes[bi + 2] = (byte) (value >> 40);
            bytes[bi + 3] = (byte) (value >> 32);
            bytes[bi + 4] = (byte) (value >> 24);
            bytes[bi + 5] = (byte) (value >> 16);
            bytes[bi + 6] = (byte) (value >> 8);
            bytes[bi + 7] = (byte) (value);
        }
        return bytes;
    }

    public static int[] byteArrayToIntArray(byte[] bytes) {
        assert(bytes.length % 4 == 0);
        int[] ints = new int[bytes.length / 4];
        for (int i = 0; i < ints.length; i++) {
            int bi = i * 4;
            ints[i] = ((bytes[bi] & 0xFF) << 24) |
                    ((bytes[bi + 1] & 0xFF) << 16) |
                    ((bytes[bi + 2] & 0xFF) << 8 ) |
                    ((bytes[bi + 3] & 0xFF));
        }
        return ints;
    }

    public static long[] byteArrayToLongArray(byte[] bytes) {
        assert(bytes.length % 8 == 0);
        long[] longs = new long[bytes.length / 8];
        for (int i = 0; i < longs.length; i++) {
            int bi = i * 8;
            longs[i] = ((long)(bytes[bi] & 0xFF) << 56) |
                    ((long)(bytes[bi + 1] & 0xFF) << 48) |
                    ((long)(bytes[bi + 2] & 0xFF) << 40) |
                    ((long)(bytes[bi + 3] & 0xFF) << 32) |
                    ((long)(bytes[bi + 4] & 0xFF) << 24) |
                    ((long)(bytes[bi + 5] & 0xFF) << 16) |
                    ((long)(bytes[bi + 6] & 0xFF) << 8 ) |
                    ((long)(bytes[bi + 7] & 0xFF));
        }
        return longs;
    }

    public static byte[] toByteArray(int value) {
        return new byte[] {
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte)value };
    }

    public static int fromByteArray(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8 ) |
                ((bytes[3] & 0xFF));
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
            byte[] lenArray = toByteArray(len);
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
            int len = fromByteArray(lenArray);
            byte[] strArray = new byte[len];
            System.arraycopy(bytes, i + 4, strArray, 0, len);
            strList.add(new String(strArray));
            i += len + 4;
        }
        return strList.toArray(new String[0]);
    }

    public static Method getFunctionMethod(Object o, String targetName) {
        for (Method m: o.getClass().getDeclaredMethods()) {
            String name = m.getName();
            if (name.equals(targetName) && Modifier.isPublic(m.getModifiers())) {
                return m;
            }
        }
        return null;
    }

    public static String getFunctionClassName(ApiaryFunction func) {
        String[] actualNames = func.getClassName().split("\\.");
        return actualNames[actualNames.length-1];
    }

    public static long getMicroTimestamp() {
        return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    }

    public static int getQueryType(String query) {
        int res;
        if (query.contains("INSERT") || query.contains("UPSERT")) {
            res = ProvenanceBuffer.ExportOperation.INSERT.getValue();
        } else if (query.contains("DELETE")) {
            res = ProvenanceBuffer.ExportOperation.DELETE.getValue();
        } else if (query.contains("UPDATE")) {
            res = ProvenanceBuffer.ExportOperation.UPDATE.getValue();
        } else {
            res = ProvenanceBuffer.ExportOperation.READ.getValue();
        }
        return res;
    }

    public static ExecuteFunctionReply.Builder constructReply(
            long callerID, long functionID, long senderTimestampNano, Object output,
            String errorMsg) {
        ExecuteFunctionReply.Builder b = ExecuteFunctionReply.newBuilder()
                .setCallerId(callerID)
                .setFunctionId(functionID)
                .setSenderTimestampNano(senderTimestampNano);
        if (output instanceof String) {
            b.setReplyType(stringType);
            b.setReplyString((String) output);
        } else if (output instanceof Integer) {
            b.setReplyType(intType);
            b.setReplyInt((int) output);
        } else if (output instanceof String[]) {
            b.setReplyType(stringArrayType);
            b.setReplyArray(ByteString.copyFrom(stringArraytoByteArray((String[]) output)));
        } else if (output instanceof int[]) {
            b.setReplyType(intArrayType);
            b.setReplyArray(ByteString.copyFrom(intArrayToByteArray((int[]) output)));
        }
        if (errorMsg != null) {
            b.setErrorMsg(errorMsg);
        }
        return b;
    }

    public static Object getOutputFromReply(ExecuteFunctionReply rep) {
        Object output = null;
        if (rep.getReplyType() == stringType) {
            output = rep.getReplyString();
        } else if (rep.getReplyType() == intType) {
            output = rep.getReplyInt();
        } else if (rep.getReplyType() == stringArrayType) {
            output = Utilities.byteArrayToStringArray(rep.getReplyArray().toByteArray());
        } else if (rep.getReplyType() == intArrayType) {
            output = Utilities.byteArrayToIntArray(rep.getReplyArray().toByteArray());
        }
        return output;
    }

    public static Object[] getArgumentsFromRequest(ExecuteFunctionRequest req) {
        List<ByteString> byteArguments = req.getArgumentsList();
        List<Integer> argumentTypes = req.getArgumentTypesList();
        Object[] arguments = new Object[byteArguments.size()];
        List<Integer> argSizes = new ArrayList<>();
        for (int i = 0; i < arguments.length; i++) {
            byte[] byteArray = byteArguments.get(i).toByteArray();
            argSizes.add(byteArray.length);
            if (argumentTypes.get(i) == stringType) {
                arguments[i] = new String(byteArray);
            } else if (argumentTypes.get(i) == intType) {
                arguments[i] = Utilities.fromByteArray(byteArray);
            } else if (argumentTypes.get(i) == stringArrayType) {
                arguments[i] = Utilities.byteArrayToStringArray(byteArray);
            }  else if (argumentTypes.get(i) == intArrayType) {
                arguments[i] = Utilities.byteArrayToIntArray(byteArray);
            }
        }
        return arguments;
    }

    public static void prettyPrint(ResultSet resultSet) {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();

            // Retrieve and store column names
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }

            // Print column names
            for (String columnName : columnNames) {
                System.out.printf("%-25s", columnName);
            }
            System.out.println();

            // Print underline for column names
            for (int i = 0; i < columnCount; i++) {
                System.out.print("------------------------");
            }
            System.out.println();

            // Iterate and print rows
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    Object value = resultSet.getObject(i);
                    String valueStr = (value == null) ? "null" : value.toString();
                    System.out.printf("%-25s", valueStr);
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
