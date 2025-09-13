package utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static utils.Constants.*;

public class Utils {
    public static int getCorrelationId(InputStream inputStream) throws IOException {
        byte[] messageBytes = inputStream.readNBytes(messageSize);
        byte[] reqApiKeyBytes = inputStream.readNBytes(reqApiKeySize);
        byte[] reqApiVersionBytes = inputStream.readNBytes(reqApiVersionSize);
        byte[] correlationIdBytes = inputStream.readNBytes(correlationIdSize);
        return ByteBuffer.wrap(correlationIdBytes).getInt();
    }

    public static short getReqApiVersion(InputStream inputStream) throws IOException {
        byte[] messageBytes = inputStream.readNBytes(messageSize);
        byte[] reqApiKeyBytes = inputStream.readNBytes(reqApiKeySize);
        byte[] reqApiVersionBytes = inputStream.readNBytes(reqApiVersionSize);
        return ByteBuffer.wrap(reqApiVersionBytes).getShort();
    }
}
