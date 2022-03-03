package org.dbos.apiary.worker;

import org.zeromq.ZFrame;

public class OutgoingReplyMsg {
    public final ZFrame address;   // Or provide address, which will be reply.
    public final byte[] output;

    public OutgoingReplyMsg(ZFrame address, byte[] output) {
        this.address = address;
        this.output = output;
    }

}
