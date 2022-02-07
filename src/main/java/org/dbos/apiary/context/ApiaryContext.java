package org.dbos.apiary.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

import java.io.IOException;

public class ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryContext.class);
    public final Client client;

    public ApiaryContext(String hostname, Integer port) throws IOException {
        ClientConfig config = new ClientConfig();
        this.client = ClientFactory.createClient(config);
        client.createConnection(hostname, port);
    }
}
