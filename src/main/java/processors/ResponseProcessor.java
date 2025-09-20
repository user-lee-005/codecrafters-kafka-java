package processors;

import dto.KafkaRequest;
import dto.KafkaResponse;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static utils.Constants.*;
import static utils.Constants.unsupportedVersionErrorCode;

public class ResponseProcessor {
    private final List<KafkaResponse.ApiVersionDTO> supportedApis;

    public ResponseProcessor(List<KafkaResponse.ApiVersionDTO> supportedApis) {
        this.supportedApis = supportedApis;
    }

    public byte[] generateResponse(KafkaRequest kafkaRequest) {
        byte[] clientIdBytes = kafkaRequest.getClientId() != null ? kafkaRequest.getClientId().getBytes(StandardCharsets.UTF_8) : new byte[0];
        int remainingSize = getRemainingSize(kafkaRequest);
        ByteBuffer buf = ByteBuffer.allocate( messageSize + remainingSize);
        writeMessageSize(buf, remainingSize);
        writeCorrelationId(buf, kafkaRequest.getCorrelationId());
        writeErrorCode(buf, kafkaRequest.getApiVersion());
        writeApiVersionsArray(buf, kafkaRequest.getApiVersion());
        return buf.array();
    }

    private int getRemainingSize(KafkaRequest kafkaRequest) {
        int remainingSize;
        // Correctly calculate size based on the protocol version
        if (kafkaRequest.getApiVersion() >= 3) {
            // Flexible version size calculation
            int arrayHeaderSize = sizeOfUnsignedVarInt(this.supportedApis.size() + 1);
            int taggedFieldsSize = sizeOfUnsignedVarInt(0);
            remainingSize = correlationIdSize // Correlation ID
                    + errorCodeSize // Error Code
                    + throttleTimeMsSize // ThrottleTimeMs
                    + arrayHeaderSize
                    + (this.supportedApis.size() * 7) // Array Data 2 + 2 + 2 + 1 (apiKey + minVersion + maxVersion + taggedBuffer)
                    + taggedFieldsSize;
        } else {
            // Rigid version size calculation
            remainingSize = correlationIdSize // Correlation ID
                    + errorCodeSize // Error Code
                    + INT_32_LENGTH // Array Length (int32)
                    + (supportedApis.size() * 6); // Array Data
        }
        return remainingSize;
    }

    private void writeErrorCode(ByteBuffer buf, short reqApiVersion) {
        if(reqApiVersion <= 4) {
            buf.putShort((short) 0);
        } else {
            buf.putShort(unsupportedVersionErrorCode);
        }
    }

    private void writeMessageSize(ByteBuffer buf, int messageSize) {
        buf.putInt(messageSize);
    }

    private void writeCorrelationId(ByteBuffer buf, int correlationId) {
        buf.putInt(correlationId);
    }

    public void writeToOutputStream(Socket clientSocket, byte[] res) throws IOException {
        clientSocket.getOutputStream().write(res);
    }

    private void writeApiVersionsArray(ByteBuffer buf, short reqApiVersion) {
        // For flexible versions (v3+), write the length as a U_VARINT.
        if (reqApiVersion >= 3) {
            writeUnsignedVarInt(supportedApis.size() + 1, buf); // Flexible arrays are length + 1
        }
        // For old, rigid versions, write the length as a 4-byte integer.
        else {
            buf.putInt(supportedApis.size());
        }

        for (KafkaResponse.ApiVersionDTO api : supportedApis) {
            buf.putShort(api.getApiKey());
            buf.putShort(api.getMinVersion());
            buf.putShort(api.getMaxVersion());
        }

        // For flexible versions, you must also write the tagged fields count (0 in this case)
        if (reqApiVersion >= 3) {
            writeUnsignedVarInt(0, buf);
        }
    }

    /**
     * Writes an integer to the buffer as an Unsigned Varint.
     */
    private static void writeUnsignedVarInt(int value, ByteBuffer buf) {
        while ((value & 0xFFFFFF80) != 0L) {
            byte b = (byte) ((value & 0x7F) | 0x80);
            buf.put(b);
            value >>>= 7;
        }
        buf.put((byte) value);
    }

    private static int sizeOfUnsignedVarInt(int value) {
        int bytes = 1;
        while ((value & 0xFFFFFF80) != 0L) {
            bytes++;
            value >>>= 7;
        }
        return bytes;
    }
}
