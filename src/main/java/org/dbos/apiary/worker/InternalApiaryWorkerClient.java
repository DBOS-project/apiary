package org.dbos.apiary.worker;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.executor.FunctionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

// Note: ZMQ.Socket is not thread-safe, so this class is not thread-safe either.
public class InternalApiaryWorkerClient extends ApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(InternalApiaryWorkerClient.class);

    public InternalApiaryWorkerClient(ZContext context) {
        super(context);
    }

    public static byte[] serializeExecuteRequest(String name, String service, long execID, long callerID, int taskID, Object... arguments) {
        return internalSerializeExecuteRequest(name, service, execID, callerID, taskID, arguments);
    }

    public FunctionOutput executeFunction(String address, String name, String service, long execID, Object... arguments) throws InvalidProtocolBufferException {
        return internalExecuteFunction(address, name, service, execID, 0l, 0, arguments);
    }

}
