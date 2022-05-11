package org.dbos.apiary.worker;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.function.FunctionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ApiaryWorkerClient provides interface for invoking Apiary functions from a remote client.
 * Note that this class is not thread-safe (due to ZMQ.Socket), thus cannot be shared between threads.
 */
public class ApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerClient.class);

    private final InternalApiaryWorkerClient internalClient;

    // A map that stores unique execution ID for each service.
    private final static Map<String, AtomicLong> serviceExecutionIdMap = new ConcurrentHashMap<>();

    public ApiaryWorkerClient() {
        internalClient = new InternalApiaryWorkerClient(new ZContext());
    }

    public ApiaryWorkerClient(ZContext zContext) {
        internalClient = new InternalApiaryWorkerClient(zContext);
    }
    
    // This can be used by asynchronous client.
    public ZMQ.Socket getSocket(String address) {
        return internalClient.getSocket(address);
    }

    public static byte[] serializeExecuteRequest(String name, String service, Object... arguments) {
        return InternalApiaryWorkerClient.serializeExecuteRequest(name, service, getExecutionId(service), 0l, 0l, arguments);
    }


    // Synchronous blocking invocation, supposed to be used by client/loadgen.
    public FunctionOutput executeFunction(String address, String name, String service, Object... arguments) throws InvalidProtocolBufferException {
        return internalClient.executeFunction(address, name, service, getExecutionId(service),  arguments);
    }

    /* --------------------------- Internal functions ------------------------------- */
    private static long getExecutionId(String service) {
        if (!serviceExecutionIdMap.containsKey(service)) {
            serviceExecutionIdMap.put(service, new AtomicLong(0));
        }
        return serviceExecutionIdMap.get(service).getAndIncrement();
    }
}
