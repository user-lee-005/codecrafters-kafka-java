package processors;

import dto.KafkaRequest;
import dto.KafkaRequestBody;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RequestProcessor {
    public KafkaRequest processRequest(DataInputStream dataInputStream) throws IOException {
        long threadId = Thread.currentThread().threadId();
        int messageSize = dataInputStream.readInt();
        int bytesConsumed = 0;

        short apiKey = dataInputStream.readShort();
        bytesConsumed += 2;

        short apiVersion = dataInputStream.readShort();
        bytesConsumed += 2;

        int correlationId = dataInputStream.readInt();
        bytesConsumed += 4;

        short clientIdLength = dataInputStream.readShort();
        bytesConsumed += 2;

        String clientId = "";
        if (clientIdLength > 0) {
            byte[] clientIdBytes = new byte[clientIdLength];
            dataInputStream.readFully(clientIdBytes);
            clientId = new String(clientIdBytes, StandardCharsets.UTF_8);
            bytesConsumed += clientIdLength;
        }

        System.out.println(MessageFormat.format(">>> Thread here line 40: {0}", bytesConsumed));

        int remainingBytes = messageSize - bytesConsumed;
        Result result = null;
        if (remainingBytes > 0) {
            if(apiKey == 75) result = getResult(dataInputStream, bytesConsumed, messageSize);
            else if (apiKey == 18) {
                result = new Result(null, remainingBytes);
            }
            if (result != null && result.remainingBytes() > 0) {
                byte[] junk = new byte[result.remainingBytes()];
                dataInputStream.readFully(junk);
            }
        }

        System.out.println("[Thread " + threadId + "] -> processRequest: Finished parsing. Returning request object.");
        if(result != null && result.body() != null) return new KafkaRequest(messageSize, correlationId, apiKey, apiVersion, clientId, result.body());
        return new KafkaRequest(messageSize, correlationId, apiKey, apiVersion, clientId);
    }

    private Result getResult(DataInputStream dataInputStream, int bytesConsumed, int messageSize) throws IOException {
        KafkaRequestBody body;
        int remainingBytes;
        dataInputStream.readByte(); // tag buffer
        bytesConsumed += 1;
        int topicsArrayLength = dataInputStream.readByte();
        bytesConsumed += 1;


        List<String> topics = new ArrayList<>();
        for (int i = 0; i < topicsArrayLength - 1; i++) {
            int topicNameLength = dataInputStream.readByte();
            bytesConsumed += 1;
            String topicName = new String(dataInputStream.readNBytes(topicNameLength - 1), StandardCharsets.UTF_8);
            topics.add(topicName);
            bytesConsumed += topicNameLength;
            dataInputStream.readByte();
            bytesConsumed += 1;
        }

        int responsePartitionLimit = dataInputStream.readInt();
        bytesConsumed += 4;

        byte cursor = dataInputStream.readByte();
        bytesConsumed += 1;

        body = new KafkaRequestBody(topics, responsePartitionLimit, cursor);

        remainingBytes = messageSize - bytesConsumed;
        return new Result(body, remainingBytes);
    }

    private record Result(KafkaRequestBody body, int remainingBytes) {
    }
}
