package org.dbos.apiary.worker;

import org.zeromq.ZFrame;

public class OutgoingRequestMsg {
    public final String hostname;
    public final byte[] output;

    public OutgoingRequestMsg(String hostname, byte[] output) {
        this.hostname = hostname;
        this.output = output;
    }
}
