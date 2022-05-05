package org.dbos.apiary.postgresdemo;

public class WebPost {
    private int postId;
    private String sender;
    private String receiver;
    private String postbody;

    public int getPostId() { return postId; }
    public void setPostId(int postId) { this.postId = postId; }

    public String getSender() { return sender; }
    public void setSender(String sender) { this.sender = sender; }

    public String getReceiver() { return receiver; }
    public void setReceiver(String receiver) { this.receiver = receiver; }

    public String getPostbody() { return postbody; }
    public void setPostbody(String postbody) { this.postbody = postbody; }
}
