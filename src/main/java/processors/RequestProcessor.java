package processors;

import dto.KafkaRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RequestProcessor {
    // In your RequestProcessor class

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
        int remainingBytes = messageSize - bytesConsumed;

        if (remainingBytes > 0) {
            byte[] junk = new byte[remainingBytes];
            dataInputStream.readFully(junk);
        }

        System.out.println("[Thread " + threadId + "] -> processRequest: Finished parsing. Returning request object.");
        return new KafkaRequest(messageSize, correlationId, apiKey, apiVersion, clientId);
    }
}
