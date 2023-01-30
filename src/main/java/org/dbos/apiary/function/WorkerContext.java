package org.dbos.apiary.function;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.procedures.postgres.GetApiaryClientID;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class WorkerContext {
    public final Map<String, ApiarySecondaryConnection> secondaryConnections = new HashMap<>();
    private final Map<String, Callable<ApiaryFunction>> functions = new HashMap<>();
    private final Map<String, String> functionTypes = new HashMap<>();

    // Record a mapping between the old function name and its new class name. Used by retroactive programming.
    private final Map<String, String> retroFunctions = new HashMap<>();

    // Record a mapping between the name of the first function and the set of functions in a workflow. Used by retroactive programming.
    private final Map<String, List<String>> functionSets = new HashMap<>();

    // Record if a function is read-only.
    private final Map<String, Boolean> functionReadOnly = new HashMap<>();

    private ApiaryConnection primaryConnection = null;
    private String primaryConnectionType;

    public final ProvenanceBuffer provBuff;

    public WorkerContext(ProvenanceBuffer provBuff) {
        this.provBuff = provBuff;
    }

    public void registerConnection(String type, ApiaryConnection connection) {
        assert(primaryConnection == null);
        primaryConnection = connection;
        primaryConnectionType = type;
        if (type.equals(ApiaryConfig.postgres)) {
            registerFunction(ApiaryConfig.getApiaryClientID, ApiaryConfig.postgres, GetApiaryClientID::new);
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
    }

    public void registerFunction(String name, String type, Callable<ApiaryFunction> function, boolean isRetro, boolean isReadOnly) {
        registerFunction(name, type, function);
        functionReadOnly.put(name, isReadOnly);
        if (isRetro) {
            // If isRetro is true, then we need to remember it in the map, so we can track which function is the modified ones.
            ApiaryFunction func = getFunction(name);
            assert (func != null);
            String actualName = Utilities.getFunctionClassName(func);
            retroFunctions.put(name, actualName);
        }
    }

    public void registerFunctionSet(String firstFunc, String[] funcNames) {
        functionSets.put(firstFunc, List.of(funcNames));
    }

    public List<String> getFunctionSet(String firstFunc) {
        return functionSets.get(firstFunc);
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
