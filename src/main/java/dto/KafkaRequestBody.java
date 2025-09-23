package dto;

import java.util.List;

public class KafkaRequestBody {
    private List<String> topics;
    private int responsePartitionLimit;
    private byte cursor; // -1 if absent

    public KafkaRequestBody() {}

    public KafkaRequestBody(List<String> topics, int responsePartitionLimit, byte cursor) {
        this.topics = topics;
        this.responsePartitionLimit = responsePartitionLimit;
        this.cursor = cursor;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public int getResponsePartitionLimit() {
        return responsePartitionLimit;
    }

    public void setResponsePartitionLimit(int responsePartitionLimit) {
        this.responsePartitionLimit = responsePartitionLimit;
    }

    public byte getCursor() {
        return cursor;
    }

    public void setCursor(byte cursor) {
        this.cursor = cursor;
    }

    @Override
    public String toString() {
        return "KafkaRequestBody{" +
                "topics=" + topics +
                ", responsePartitionLimit=" + responsePartitionLimit +
                ", cursor=" + cursor +
                '}';
    }
}
