package processors;

import dto.KafkaRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RequestProcessor {
    public KafkaRequest processRequest(InputStream inputStream) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream));
        int messageSize = dataInputStream.readInt();
        short apiKey = dataInputStream.readShort();
        short apiVersion = dataInputStream.readShort();
        int correlationId = dataInputStream.readInt();
        short clientIdLength = dataInputStream.readShort();
        String clientId = "";
        if(clientIdLength > 0) {
            byte[] clientIdBytes = new byte[clientIdLength];
            dataInputStream.readFully(clientIdBytes);
            clientId = new String(clientIdBytes, StandardCharsets.UTF_8);
        }
        return new KafkaRequest(messageSize, correlationId, apiKey, apiVersion, clientId);
    }
}
