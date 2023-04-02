package org.dbos.apiary.rsademo;

public class WebPost {
    private String sender;
    private String receiver;
    private String postText;

    public String getSender() { return sender; }
    public void setSender(String sender) { this.sender = sender; }

    public String getReceiver() { return receiver; }
    public void setReceiver(String receiver) { this.receiver = receiver; }

    public String getPostText() { return postText; }
    public void setPostText(String postText) { this.postText = postText; }
}
