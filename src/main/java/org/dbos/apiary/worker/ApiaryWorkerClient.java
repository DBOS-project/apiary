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
    private final String apiaryWorkerAddress;
    private final int clientID;

    // A map that stores unique execution ID for each service.
    private final static AtomicLong execIDGenerator = new AtomicLong(0);

    public ApiaryWorkerClient(String apiaryWorkerAddress) {
        this(apiaryWorkerAddress, new ZContext());
    }

    public ApiaryWorkerClient(String apiaryWorkerAddress, ZContext zContext) {
        this.apiaryWorkerAddress = apiaryWorkerAddress;
        this.internalClient = new InternalApiaryWorkerClient(zContext);
        int tmpID = 0;
        try {
            tmpID = internalClient.executeFunction(this.apiaryWorkerAddress, "GetApiaryClientID", "ApiarySystem", 0l).getInt();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        this.clientID = tmpID;
    }
    
    // This can be used by asynchronous client.
    public ZMQ.Socket getSocket(String address) {
        return internalClient.getSocket(address);
    }

    public static byte[] serializeExecuteRequest(String name, String service, Object... arguments) {
        return InternalApiaryWorkerClient.serializeExecuteRequest(name, service, getExecutionId(), 0L, 0L, arguments);
    }


    // Synchronous blocking invocation, supposed to be used by client/loadgen.
    public FunctionOutput executeFunction(String name, String service, Object... arguments) throws InvalidProtocolBufferException {
        return internalClient.executeFunction(this.apiaryWorkerAddress, name, service, getExecutionId(), arguments);
    }

    /* --------------------------- Internal functions ------------------------------- */
    private static long getExecutionId() {
        return execIDGenerator.incrementAndGet();
    }
}
