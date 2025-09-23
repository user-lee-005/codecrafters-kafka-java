package dto;

public class KafkaRequest {
    private int messageSize;
    private int correlationId;
    private short apiKey;
    private short apiVersion;
    private String clientId;
    private KafkaRequestBody body;

    public KafkaRequest(int messageSize, int correlationId, short apiKey, short apiVersion, String clientId, KafkaRequestBody body) {
        this.messageSize = messageSize;
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.clientId = clientId;
        this.body = body;
    }

    public KafkaRequest(int messageSize, int correlationId, short apiKey, short apiVersion, String clientId) {
        this.messageSize = messageSize;
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.clientId = clientId;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public short getApiKey() {
        return apiKey;
    }

    public void setApiKey(short apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public String toString() {
        if(body != null) return "KafkaRequest{" +
                "messageSize=" + messageSize +
                ", correlationId=" + correlationId +
                ", apiKey=" + apiKey +
                ", apiVersion=" + apiVersion +
                ", clientId='" + clientId + '\'' +
                ", Kafka Request Body{" +
                body.toString() +
                "}}";
        return "KafkaRequest{" +
                "messageSize=" + messageSize +
                ", correlationId=" + correlationId +
                ", apiKey=" + apiKey +
                ", apiVersion=" + apiVersion +
                ", clientId='" + clientId + '\'' +
                "}";
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(short apiVersion) {
        this.apiVersion = apiVersion;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public KafkaRequestBody getBody() {
        return body;
    }

    public void setBody(KafkaRequestBody body) {
        this.body = body;
    }
}
