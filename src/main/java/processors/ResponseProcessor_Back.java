package processors;

import dto.KafkaRequest;
import dto.KafkaResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.List;

import static utils.Constants.*;
import static utils.Constants.unsupportedVersionErrorCode;

public class ResponseProcessor_Back {
    private final List<KafkaResponse.ApiVersionDTO> supportedApis;

    public ResponseProcessor_Back(List<KafkaResponse.ApiVersionDTO> supportedApis) {
        this.supportedApis = supportedApis;
    }

    public byte[] generateResponse(KafkaRequest kafkaRequest) {
        return switch (kafkaRequest.getApiKey()) {
            case 18 -> getApiVersionResponse(kafkaRequest);
            case 75 -> getDescribeTopicPartitionsResponse(kafkaRequest);
            default ->
                    throw new UnsupportedOperationException("API Key " + kafkaRequest.getApiKey() + " not implemented yet");
        };
    }

    /**
     * Generates a response for a DescribeTopicPartitions request (ApiKey 75).
     * This implementation simulates the scenario where the requested topic is not found.
     * It returns a response with the UNKNOWN_TOPIC_OR_PARTITION error code.
     *
     * @param kafkaRequest The request from the client.
     * @return A byte array representing the response.
     */
    private byte[] getDescribeTopicPartitionsResponse(KafkaRequest kafkaRequest) {
        String unknownTopicName = "unknown_topic";
        byte[] nullTopicId = new byte[16];

        final short FLEXIBLE_VERSION_FLAG = 3;
        int remainingSize = getDescribeTopicPartitionsResponseSize(FLEXIBLE_VERSION_FLAG, unknownTopicName);
        ByteBuffer buf = ByteBuffer.allocate(messageSize + remainingSize);
        int totalSize = messageSize + remainingSize;
        System.out.println(MessageFormat.format(">>> Allocating buffer. Total size: {0} bytes. Message body size: {1} bytes.", totalSize, remainingSize));

        // 1. Write the total size of the message (excluding this field itself).
        writeMessageSize(buf, remainingSize);
        System.out.println(MessageFormat.format(">>> After MessageSize: position={0}", buf.position()));

        // 2. Write the correlation ID to match the request.
        writeCorrelationId(buf, kafkaRequest.getCorrelationId());
        System.out.println(MessageFormat.format(">>> After CorrelationID: position={0}", buf.position()));

        // 3. Write the throttle time in milliseconds.
        buf.putInt(0);
        System.out.println(MessageFormat.format(">>> After ThrottleTime: position={0}", buf.position()));

        // 4. Write the length of the topics array. We are returning info for 1 topic.
        // In the flexible format, array length is encoded as VARINT(num_elements + 1).
        // So for 1 topic, we write VARINT(1 + 1 = 2).
        writeUnsignedVarInt(2, buf);
        System.out.println(MessageFormat.format(">>> After Topics Array Length: position={0}", buf.position()));

        // 5. Write the topic name.
        writeString(buf, unknownTopicName, FLEXIBLE_VERSION_FLAG);
        System.out.println(MessageFormat.format(">>> After Topic Name: position={0}", buf.position()));

        // 6. Write the error code for the topic.
        buf.putShort(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE);
        System.out.println(MessageFormat.format(">>> After Error Code: position={0}", buf.position()));

        // 7. Write the topic ID (UUID).
        writeBytes(buf, nullTopicId);
        System.out.println(MessageFormat.format(">>> After Topic ID: position={0}", buf.position()));

        // 8. Write the partitions array for this topic. It's empty.
        // For an empty array (0 elements), we write VARINT(0 + 1 = 1).
        writeUnsignedVarInt(1, buf);
        System.out.println(MessageFormat.format(">>> After Partitions Array Length: position={0}", buf.position()));

        // 9. Write the 'is_internal' flag.
        buf.put((byte) 0); // is_internal = false

        // 10. Write the 'topic_authorized_operations' integer.
        buf.putInt(0);

        // 11. For flexible versions, write the tagged fields count for the topic.
        writeUnsignedVarInt(0, buf);
        System.out.println(MessageFormat.format(">>> After Topic Tagged Fields: position={0}", buf.position()));

        // 12. Write the top-level tagged fields count. This is mandatory for flexible versions.
        writeUnsignedVarInt(0, buf);
        System.out.println(MessageFormat.format(">>> After Top-Level Tagged Fields (Final): position={0}", buf.position()));

        return buf.array();
    }

    private byte[] getApiVersionResponse(KafkaRequest kafkaRequest) {
        byte[] clientIdBytes = kafkaRequest.getClientId() != null ? kafkaRequest.getClientId().getBytes(StandardCharsets.UTF_8) : new byte[0];
        int remainingSize = getRemainingSize(kafkaRequest);
        ByteBuffer buf = ByteBuffer.allocate( messageSize + remainingSize);
        writeMessageSize(buf, remainingSize);
        writeCorrelationId(buf, kafkaRequest.getCorrelationId());
        writeErrorCode(buf, kafkaRequest.getApiVersion());
        writeApiVersionsArray(buf, kafkaRequest.getApiVersion(), kafkaRequest.getApiKey());
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
        OutputStream outputStream = clientSocket.getOutputStream();
        outputStream.write(res);
        System.out.println(">>>>> Writing Output");
        outputStream.flush();
    }

    private void writeApiVersionsArray(ByteBuffer buf, short reqApiVersion, short apiKey) {
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
            // For flexible versions, you must also write the tagged fields count (0 in this case)
            if (reqApiVersion >= 3) {
                writeUnsignedVarInt(0, buf);
            }
        }

//        Optional<KafkaResponse.ApiVersionDTO> apiVersionDTO = supportedApis.stream().filter(api -> api.getApiKey() == apiKey).findFirst();
//        apiVersionDTO.ifPresent(versionDTO -> {
//            buf.putShort(versionDTO.getApiKey());
//            buf.putShort(versionDTO.getMinVersion());
//            buf.putShort(versionDTO.getMaxVersion());
//
//            // For flexible versions, you must also write the tagged fields count (0 in this case)
//            if (reqApiVersion >= 3) {
//                writeUnsignedVarInt(0, buf);
//            }
//        });
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

    /**
     * Calculates the total size of the DescribeTopicPartitions response payload.
     * This is required to pre-allocate the ByteBuffer.
     *
     * @param apiVersion The API version from the request.
     * @param topicName  The name of the topic being included in the response.
     * @return The size in bytes.
     */
    private int getDescribeTopicPartitionsResponseSize(short apiVersion, String topicName) {
        int size = 0;
        int specialFieldsSize = 1 + 4; // byte + int
        size += correlationIdSize;  // 4 bytes
        size += throttleTimeMsSize; // 4 bytes

        // Size of topics array length
        size += (apiVersion >= 3) ? sizeOfUnsignedVarInt(1) : INT_32_LENGTH;

        // Size of the single topic response within the array
        size += errorCodeSize; // 2 bytes for the error code
        size += sizeOfString(topicName, apiVersion); // Size of the topic name (length + bytes)
        size += 16; // 16 bytes for the UUID (Topic ID)
        size += specialFieldsSize;

        // Size of partitions array length (for the topic)
        size += (apiVersion >= 3) ? sizeOfUnsignedVarInt(0) : INT_32_LENGTH;

        // For flexible versions, add size of tagged fields
        if (apiVersion >= 3) {
            size += sizeOfUnsignedVarInt(0);
        }

        System.out.println(MessageFormat.format("<<< Get Describe Topic Partitions Response : {0}", size));
        return size;
    }

    /**
     * Writes a string to the buffer, handling both rigid (short length) and
     * flexible (VARINT length) protocol versions.
     *
     * @param buf        The ByteBuffer to write to.
     * @param s          The string to write.
     * @param apiVersion The API version to determine the encoding.
     */
    private void writeString(ByteBuffer buf, String s, short apiVersion) {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        if (apiVersion >= 3) {
            writeUnsignedVarInt(data.length, buf);
        } else {
            buf.putShort((short) data.length);
        }
        buf.put(data);
    }

    /**
     * Writes a byte array to the buffer.
     * @param buf The ByteBuffer to write to.
     * @param bytes The byte array to write.
     */
    private void writeBytes(ByteBuffer buf, byte[] bytes) {
        buf.put(bytes);
    }


    /**
     * Calculates the size a string will occupy in the buffer, including its length prefix.
     *
     * @param s          The string to measure.
     * @param apiVersion The API version to determine the encoding.
     * @return The total size in bytes.
     */
    private int sizeOfString(String s, short apiVersion) {
        int stringLength = s.getBytes(StandardCharsets.UTF_8).length;
        System.out.println(MessageFormat.format(">>> Size of String : {0}", stringLength));
        int lengthPrefixSize = (apiVersion >= 3) ? sizeOfUnsignedVarInt(stringLength) : 2;
        System.out.println(MessageFormat.format("<<< Size of String : {0}", lengthPrefixSize + stringLength));
        return lengthPrefixSize + stringLength;
    }
}
