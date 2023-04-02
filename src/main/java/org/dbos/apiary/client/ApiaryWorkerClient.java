package org.dbos.apiary.client;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryWorker;
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

    private final String clientRole;

    // A map that stores unique execution ID for each client.
    private final AtomicLong execIDGenerator = new AtomicLong(0);

    /**
     * Create a client for sending synchronous requests to Apiary.
     * @param apiaryWorkerAddress   the address of an Apiary worker.
     */
    public ApiaryWorkerClient(String apiaryWorkerAddress) {
        this(apiaryWorkerAddress, new ZContext(), ApiaryConfig.defaultRole);
    }

    /**
     * Create a client for sending synchronous requests to Apiary, with a specified role.
     * @param apiaryWorkerAddress   the address of an Apiary worker.
     */
    public ApiaryWorkerClient(String apiaryWorkerAddress, String apiaryRole) {
        this(apiaryWorkerAddress, new ZContext(), apiaryRole);
    }

    /**
     * Create a client for sending asynchronous requests to Apiary.
     * @param apiaryWorkerAddress   the address of an Apiary worker.
     * @param zContext              the ZContext to be used for sending requests and receiving replies.
     */
    public ApiaryWorkerClient(String apiaryWorkerAddress, ZContext zContext) {
        this(apiaryWorkerAddress, zContext, ApiaryConfig.defaultRole);
    }

    /**
     * Create a client for sending asynchronous requests to Apiary, with a specified role.
     * @param apiaryWorkerAddress   the address of an Apiary worker.
     * @param zContext              the ZContext to be used for sending requests and receiving replies.
     * @param apiaryRole            the role of this client.
     */
    public ApiaryWorkerClient(String apiaryWorkerAddress, ZContext zContext, String apiaryRole) {
        this.apiaryWorkerAddress = apiaryWorkerAddress;
        this.internalClient = new InternalApiaryWorkerClient(zContext);
        this.clientRole = apiaryRole;
        int tmpID = 0;
        try {
            tmpID = internalClient.executeFunction(this.apiaryWorkerAddress, getApiaryClientID, ApiaryConfig.systemRole, 0L, ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()).getInt();
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
     * @param role      the role name that invokes this function.
     * @param arguments the arguments of the invoked function.
     * @return          serialized byte array of the request.
     */
    public byte[] serializeExecuteRequest(String name, String role, Object... arguments) {
        return InternalApiaryWorkerClient.serializeExecuteRequest(name, role, getExecutionId(), ApiaryConfig.ReplayMode.NOT_REPLAY.getValue(), 0L, 0L, arguments);
    }

    /**
     * Invoke a function synchronously and block waiting for the result.
     * @param name      the name of the invoked function.
     * @param arguments the arguments of the invoked function.
     * @return          the output of the invoked function.
     * @throws InvalidProtocolBufferException
     */
    public FunctionOutput executeFunction(String name, Object... arguments) throws InvalidProtocolBufferException {
        return internalClient.executeFunction(this.apiaryWorkerAddress, name, this.clientRole, getExecutionId(), ApiaryConfig.ReplayMode.NOT_REPLAY.getValue(), arguments);
    }

    /**
     * Replay the execution and everything after it using the original execution trace. Block waiting for the result of the last execution. The replay will not generate new provenance data.
     *
     * @param startExecId    the original execution ID of the target request.
     * @param endExecId the execution ID of a request past the last request of the replay, excluded from replay.
     * @param retroMode the mode of replay. See {@link ApiaryConfig.ReplayMode}
     * @return the output of the last execution.
     * @throws InvalidProtocolBufferException
     */
    public FunctionOutput retroReplay(long startExecId, long endExecId, int retroMode) throws InvalidProtocolBufferException {
        return internalClient.retroReplay(this.apiaryWorkerAddress, "retroReplay", this.clientRole, startExecId, endExecId, retroMode, null);
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
