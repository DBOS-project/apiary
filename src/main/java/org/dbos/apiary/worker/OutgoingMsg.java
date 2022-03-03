package org.dbos.apiary.worker;

import org.zeromq.ZFrame;

public class OutgoingMsg {
    public final String hostname;  // Either provide hostname, which will be request.
    public final ZFrame address;   // Or provide address, which will be reply.
    public final byte[] output;

    public OutgoingMsg(ZFrame address, byte[] output) {
        this.address = address;
        this.output = output;
        this.hostname = null;
    }

    public OutgoingMsg(String hostname, byte[] output) {
        this.hostname = hostname;
        this.output = output;
        this.address = null;
    }
}
