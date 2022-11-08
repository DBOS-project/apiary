package org.dbos.apiary.client;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.util.concurrent.atomic.AtomicLong;

import static org.dbos.apiary.utilities.ApiaryConfig.getApiaryClientID;

/**
 * ApiaryWorkerClient provides an interface for invoking Apiary functions from a remote client.
 * This class is not thread-safe.
 */
public class ApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerClient.class);

    private final InternalApiaryWorkerClient internalClient;
    private final String apiaryWorkerAddress;
    private final int clientID;

    // A map that stores unique execution ID for each service.
    private final AtomicLong execIDGenerator = new AtomicLong(0);

    /**
     * Create a client for sending synchronous requests to Apiary.
     * @param apiaryWorkerAddress   the address of an Apiary worker.
     */
    public ApiaryWorkerClient(String apiaryWorkerAddress) {
        this(apiaryWorkerAddress, new ZContext());
    }

    /**
     * Create a client for sending asynchronous requests to Apiary.
     * @param apiaryWorkerAddress   the address of an Apiary worker.
     * @param zContext              the ZContext to be used for sending requests and receiving replies.
     */
    public ApiaryWorkerClient(String apiaryWorkerAddress, ZContext zContext) {
        this.apiaryWorkerAddress = apiaryWorkerAddress;
        this.internalClient = new InternalApiaryWorkerClient(zContext);
        int tmpID = 0;
        try {
            tmpID = internalClient.executeFunction(this.apiaryWorkerAddress, getApiaryClientID, "ApiarySystem", 0L, ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()).getInt();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        this.clientID = tmpID;
    }

    /**
     * Get a ZMQ socket for sending asynchronous requests.
     * @param address   the address of an Apiary worker.
     * @return          a {@link ZMQ.Socket} object that can be used to send requests.
     */
    public ZMQ.Socket getSocket(String address) {
        return internalClient.getSocket(address);
    }

    /**
     * Serialize a function invocation request, used for sending asynchronous requests.
     * @param name      the name of the invoked function.
     * @param service   the service name of this invocation.
     * @param arguments the arguments of the invoked function.
     * @return          serialized byte array of the request.
     */
    public byte[] serializeExecuteRequest(String name, String service, Object... arguments) {
        return InternalApiaryWorkerClient.serializeExecuteRequest(name, service, getExecutionId(), ApiaryConfig.ReplayMode.NOT_REPLAY.getValue(), 0L, 0L, arguments);
    }

    /**
     * Invoke a function synchronously and block waiting for the result.
     * @param name      the name of the invoked function.
     * @param arguments the arguments of the invoked function.
     * @return          the output of the invoked function.
     * @throws InvalidProtocolBufferException
     */
    public FunctionOutput executeFunction(String name, Object... arguments) throws InvalidProtocolBufferException {
        return internalClient.executeFunction(this.apiaryWorkerAddress, name, "DefaultService", getExecutionId(), ApiaryConfig.ReplayMode.NOT_REPLAY.getValue(), arguments);
    }

    /**
     * Replay a single function/workflow synchronously and block waiting for the result. The replay will not generate new provenance data.
     * @param execId    the original execution ID of the invoked function.
     * @param name      the name of the invoked function.
     * @param arguments the original arguments of the invoked function.
     * @return          the output of the invoked function, which should be identical to the original one.
     * @throws InvalidProtocolBufferException
     */
    public FunctionOutput replayFunction(long execId, String name, Object... arguments) throws InvalidProtocolBufferException {
        return internalClient.executeFunction(this.apiaryWorkerAddress, name, "DefaultService", execId, ApiaryConfig.ReplayMode.SINGLE.getValue(), arguments);
    }


    public FunctionOutput retroReplay(long execId) throws InvalidProtocolBufferException {
        return internalClient.executeFunction(this.apiaryWorkerAddress, "retroReplay", "DefaultService", execId, ApiaryConfig.ReplayMode.ALL.getValue(), null);
    }

    /**
     * Get the globally unique clientID of this current client.
     * @return  the unique ID of this client.
     */
    public int getClientID() { return this.clientID; }

    /* --------------------------- Internal functions ------------------------------- */
    private long getExecutionId() {
        return ((long)this.clientID << 48) + execIDGenerator.getAndIncrement();
    }
}
