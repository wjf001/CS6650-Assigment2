public class ChatMessage {

    public String userId;
    public String username;
    public String message;
    public String messageType;

    public String messageId;
    public String roomId;
    public String serverId;
    public String clientIp;
    public String timestamp;

    public ChatMessage() {}

    public ChatMessage(String userId, String username, String message, String messageType) {
        this.userId      = userId;
        this.username    = username;
        this.message     = message;
        this.messageType = messageType;
    }

    @Override
    public String toString() {
        return "ChatMessage{messageId=" + messageId
                + ", roomId=" + roomId
                + ", userId=" + userId
                + ", messageType=" + messageType + "}";
    }
}