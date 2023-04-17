package org.dbos.apiary.function;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.procedures.postgres.GetApiaryClientID;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class WorkerContext {
    public final Map<String, ApiarySecondaryConnection> secondaryConnections = new HashMap<>();
    private final Map<String, Callable<ApiaryFunction>> functions = new HashMap<>();
    private final Map<String, String> functionTypes = new HashMap<>();

    // Record authorization information.
    private final Map<String, Set<String>> functionRoles = new HashMap<>();
    private final Set<String> suspendedRoles = new HashSet<>();

    // Record a mapping between the old function name and its new class name. Used by retroactive programming.
    private final Map<String, String> retroFunctions = new HashMap<>();

    // Record a mapping between the name of the first function and the set of functions in a workflow. Used by retroactive programming.
    private final Map<String, List<String>> functionSets = new HashMap<>();

    // Record if a function is read-only.
    private final Map<String, Boolean> functionReadOnly = new HashMap<>();

    // Record tables a function may access.
    private final Map<String, List<String>> functionReadTables = new HashMap<>();
    private final Map<String, List<String>> functionWriteTables = new HashMap<>();

    private ApiaryConnection primaryConnection = null;
    private String primaryConnectionType;

    public final ProvenanceBuffer provBuff;
    public String provDBType;
    public String provAddress;
    public int numWorkersThreads;

    public WorkerContext(ProvenanceBuffer provBuff) {
        this.provBuff = provBuff;
    }

    public void registerConnection(String type, ApiaryConnection connection) {
        assert(primaryConnection == null);
        primaryConnection = connection;
        primaryConnectionType = type;
        if (type.equals(ApiaryConfig.postgres)) {
            registerFunction(ApiaryConfig.getApiaryClientID, ApiaryConfig.postgres, GetApiaryClientID::new);
            if (this.provBuff != null) {
                this.provBuff.pgConn = ThreadLocal.withInitial(() -> connection.createNewConnection());
            }
        } else if (type.equals(ApiaryConfig.voltdb)) {
            registerFunction(ApiaryConfig.getApiaryClientID, ApiaryConfig.voltdb, org.dbos.apiary.procedures.voltdb.GetApiaryClientID::new);
        }
    }

    public void registerConnection(String type, ApiarySecondaryConnection connection) {
        secondaryConnections.put(type, connection);
    }

    public void registerFunction(String name, String type, Callable<ApiaryFunction> function) {
        functions.put(name, function);
        functionTypes.put(name, type);
        boolean isReadOnly;
        List<String> readTables;
        List<String> writeTables;
        try {
            isReadOnly = function.call().isReadOnly();
            readTables = function.call().readTables();
            readTables = readTables.stream().map(String::toUpperCase).collect(Collectors.toList());
            writeTables = function.call().writeTables();
            writeTables = writeTables.stream().map(String::toUpperCase).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        functionReadOnly.put(name, isReadOnly);
        functionReadTables.put(name, readTables);
        functionWriteTables.put(name, writeTables);
    }

    public void registerFunction(String name, String type, Callable<ApiaryFunction> function, boolean isRetro) {
        registerFunction(name, type, function);
        if (isRetro) {
            // If isRetro is true, then we need to remember it in the map, so we can track which function is the modified ones.
            ApiaryFunction func = getFunction(name);
            assert (func != null);
            String actualName = Utilities.getFunctionClassName(func);
            retroFunctions.put(name, actualName);
        }
    }

    public void restrictFunction(String name, Set<String> roles) {
        functionRoles.put(name, roles);
    }

    public Set<String> getFunctionRoles(String name) {
        return functionRoles.get(name);
    }

    public void suspendRole(String role) {
        suspendedRoles.add(role);
    }

    public void restoreRole(String role) {
        suspendedRoles.remove(role);
    }

    public boolean checkSuspended(String role) {
        return suspendedRoles.contains(role);
    }

    public void registerFunctionSet(String firstFunc, String[] funcNames) {
        functionSets.put(firstFunc, List.of(funcNames));
    }

    // Try to find the function set info. If not found, return the first function name.
    public List<String> getFunctionSet(String firstFunc) {
        if (functionSets.containsKey(firstFunc)) {
            return functionSets.get(firstFunc);
        }
        // Return the first function.
        return List.of(firstFunc);
    }

    public boolean getFunctionReadOnly(String func) {
        Boolean isRo = functionReadOnly.get(func);
        if (isRo == null) {
            return false;  // Conservatively return false.
        }
        return isRo;
    }

    public boolean getFunctionSetReadOnly(String firstFunc) {
        List<String> funcs = functionSets.get(firstFunc);
        Boolean isRO;
        // Return read-only info of this function, if cannot find func set info.
        if ((funcs == null) || funcs.isEmpty()) {
            isRO = functionReadOnly.get(firstFunc);
            if (isRO == null) {
                return false;
            }
            return isRO;
        }

        // Check all functions in the func set.
        for (String func : funcs) {
            isRO = functionReadOnly.get(func);
            if ((isRO == null) || !isRO) {
                return false;
            }
        }
        return true;
    }

    public Set<String> getFunctionSetTables(String firstFunc, boolean isRead) {
        Map<String, List<String>> functionAccessTables = isRead ? functionReadTables : functionWriteTables;
        List<String> funcs = functionSets.get(firstFunc);
        Set<String> tables = new HashSet<>();
        // Return table info of this function, if cannot find func set info.
        if ((funcs == null) || funcs.isEmpty()) {
            if (functionAccessTables.containsKey(firstFunc)) {
                tables.addAll(functionAccessTables.get(firstFunc));
            }
            return tables;
        }

        // Check all functions in the function set.
        for (String func : funcs) {
            if (functionAccessTables.containsKey(func)) {
                tables.addAll(functionAccessTables.get(func));
            }
        }
        return tables;
    }

    public Set<String> listAllFunctions() {
        return functions.keySet();
    }

    public Set<String> listAllFunctionSets() {
        return functionSets.keySet();
    }

    public String getFunctionType(String function) {
        return functionTypes.get(function);
    }

    public boolean functionExists(String function) {
        return functions.containsKey(function);
    }

    public boolean retroFunctionExists(String function) { return retroFunctions.containsKey(function); }

    public boolean hasRetroFunctions() { return !retroFunctions.isEmpty(); }

    public ApiaryFunction getFunction(String function) {
        try {
            return functions.get(function).call();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getPrimaryConnectionType() { return primaryConnectionType; }

    public ApiaryConnection getPrimaryConnection() { return primaryConnection; }

    public ApiarySecondaryConnection getSecondaryConnection(String db) { return secondaryConnections.get(db); }
}
