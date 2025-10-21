package processors;

import dto.KafkaRequest;
import dto.KafkaResponse;
import dto.MetadataCache;
import dto.PartitionData;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static utils.Constants.*;
import static utils.Constants.unsupportedVersionErrorCode;

public class ResponseProcessor {
    private final List<KafkaResponse.ApiVersionDTO> supportedApis;
    private final MetadataCache metadataCache;

    // A constant for the "unknown topic or partition" error code in Kafka.
    private static final short UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE = 3;

    public ResponseProcessor(List<KafkaResponse.ApiVersionDTO> supportedApis, MetadataCache metadataCache) {
        this.supportedApis = supportedApis;
        this.metadataCache = metadataCache;
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
     * Based on test logs, it must always return a response in the "flexible" format,
     * regardless of the request's API version. Includes detailed logging for each field.
     *
     * @param kafkaRequest The request from the client.
     * @return A byte array representing the response.
     */
    private byte[] getDescribeTopicPartitionsResponse(KafkaRequest kafkaRequest) {
        List<String> topics = kafkaRequest.getBody().getTopics().stream().sorted().toList();
        Map<String, List<PartitionData>> topicPartitionsMap = new HashMap<>();
        if (metadataCache != null) {
            for (String topicName : topics) {
                List<PartitionData> partitions = metadataCache.byName().get(topicName);
                topicPartitionsMap.put(topicName, partitions != null ? partitions : Collections.emptyList());
            }
        }

        final short FLEXIBLE_VERSION_FLAG = 3;
        int remainingSize = getDescribeTopicPartitionsResponseSize(FLEXIBLE_VERSION_FLAG, topics, topicPartitionsMap);
        ByteBuffer buf = ByteBuffer.allocate(messageSize + remainingSize);

        System.out.println("\n--- Building DescribeTopicPartitions Response ---");

        // 1. Write the total size of the message (excluding this field itself).
        logWrite(buf, "Message Size", remainingSize, () -> buf.putInt(remainingSize));

        // 2. Write the correlation ID to match the request.
        int correlationId = kafkaRequest.getCorrelationId();
        logWrite(buf, "Correlation ID", correlationId, () -> buf.putInt(correlationId));

        logWrite(buf, "Tagged Buffer", 0, () -> writeUnsignedVarInt(0, buf));

        // 3. Write the throttle time in milliseconds.
        logWrite(buf, "Throttle Time", 0, () -> buf.putInt(0));

        // 4. Write the topics array length using flexible format (VARINT).
        logWrite(buf, "Topics Array Length", topics.size() + 1, () -> writeUnsignedVarInt(topics.size() + 1, buf));

        for(String topicName: topics) {
            short errorCode;
            String topicUuid;
            short partitionArrayLength;
            List<PartitionData> partitionData = topicPartitionsMap.get(topicName);
            if(partitionData == null || partitionData.isEmpty()) {
                topicUuid = null;
                errorCode = UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE;
                partitionArrayLength = 0;
            } else {
                errorCode = 0;
                topicUuid = partitionData.getFirst().topicUuid();
                partitionArrayLength = (short) (partitionData.size() + 1);
            }

            // 5. Write the error code for the topic.
            logWrite(buf, "Error Code", errorCode, () -> buf.putShort(errorCode));

            // ---- Start of Topic[0] Data (Order is critical) ----
            // 6. Write the topic name.
            logWrite(buf, "Topic Name", topicName, () -> writeString(buf, topicName, FLEXIBLE_VERSION_FLAG));

            // 7. Write the topic ID (UUID).
            logWrite(buf, "Topic ID", topicUuid, () -> writeBytes(buf, uuidToBytes(topicUuid)));

            // 8. Write the 'is_internal' flag (discovered from analysis).
            logWrite(buf, "Is Internal Flag", (byte) 0, () -> buf.put((byte) 0));

            // 9. Write the partitions array for this topic. It's empty.
            logWrite(buf, "Partitions Array Length", partitionArrayLength, () -> writeUnsignedVarInt(partitionArrayLength, buf));

            if(partitionArrayLength > 0) {
                for(int i = 0; i < partitionArrayLength - 1; i ++) {
                    logWrite(buf, "Error Code", 0, () -> buf.putShort((short) 0));
                    int partitionIndex = i;
                    PartitionData ithPartitionData = partitionData.get(partitionIndex);
                    logWrite(buf, "Partition Index", partitionIndex, () -> buf.putInt(partitionIndex));
                    logWrite(buf, "Leader ID", ithPartitionData.leader(), () -> buf.putInt(ithPartitionData.leader()));
                    logWrite(buf, "Leader Epoch", ithPartitionData.leaderEpoch(), () -> buf.putInt(ithPartitionData.leaderEpoch()));
                    logWrite(buf, "Replica Nodes Array Length", ithPartitionData.replicas().size() + 1, () -> writeUnsignedVarInt(ithPartitionData.replicas().size() + 1, buf));
                    if(!ithPartitionData.replicas().isEmpty()) {
                        for (int j = 0; j < ithPartitionData.replicas().size(); j ++) {
                            int replica = ithPartitionData.replicas().get(j);
                            logWrite(buf, "Replica Node", replica, () -> buf.putInt(replica));
                        }
                    }
                    logWrite(buf, "In Sync Replica Nodes Array Length", ithPartitionData.inSyncReplicas().size() + 1, () -> writeUnsignedVarInt(ithPartitionData.replicas().size() + 1, buf));
                    if(!ithPartitionData.inSyncReplicas().isEmpty()) {
                        for (int j = 0; j < ithPartitionData.inSyncReplicas().size(); j ++) {
                            int replica = ithPartitionData.inSyncReplicas().get(j);
                            logWrite(buf, "Replica Node", replica, () -> buf.putInt(replica));
                        }
                    }
                    logWrite(buf, "Eligible Leader Replica Nodes Array Length", 1, () -> writeUnsignedVarInt(1, buf));
                    logWrite(buf, "Last Known ELR Array Length", 1, () -> writeUnsignedVarInt(1, buf));
                    logWrite(buf, "Offline Replica Nodes Array Length", 1, () -> writeUnsignedVarInt(1, buf));
                    logWrite(buf, "Topic Tagged Fields", 0, () -> writeUnsignedVarInt(0, buf));
                }
            }

            // 10. Write the 'topic_authorized_operations' integer (discovered from analysis).
            logWrite(buf, "Topic Auth Operations", 0, () -> buf.putInt(0));

            // 11. Write the tagged fields for the topic.
            logWrite(buf, "Topic Tagged Fields", 0, () -> writeUnsignedVarInt(0, buf));

        }
        buf.put((byte) 0xFF);
        buf.put((byte) 0x00);

        System.out.println("--- Response Building Complete ---\n");
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
        if (kafkaRequest.getApiVersion() >= 3) {
            int arrayHeaderSize = sizeOfUnsignedVarInt(this.supportedApis.size() + 1);
            int taggedFieldsSize = sizeOfUnsignedVarInt(0);
            remainingSize = correlationIdSize + errorCodeSize + throttleTimeMsSize + arrayHeaderSize
                    + (this.supportedApis.size() * 7) + taggedFieldsSize;
        } else {
            remainingSize = correlationIdSize + errorCodeSize + INT_32_LENGTH
                    + (supportedApis.size() * 6);
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
        if (reqApiVersion >= 3) {
            writeUnsignedVarInt(supportedApis.size() + 1, buf);
        } else {
            buf.putInt(supportedApis.size());
        }
        for (KafkaResponse.ApiVersionDTO api : supportedApis) {
            buf.putShort(api.getApiKey());
            buf.putShort(api.getMinVersion());
            buf.putShort(api.getMaxVersion());
            if (reqApiVersion >= 3) {
                writeUnsignedVarInt(0, buf);
            }
        }
    }

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

    private int getDescribeTopicPartitionsResponseSize(
            short apiVersion, List<String> topics, Map<String, List<PartitionData>> partitionsMap) {
        int size = 0;
        size += correlationIdSize;
        size += throttleTimeMsSize;
        size += sizeOfUnsignedVarInt(0); // Response header tagged buffer

        int specialFieldsSize = 1 + 4; // is_internal + topic_authorized_operations
        size += sizeOfUnsignedVarInt(topics.size()); // Topics array length
        for(String topicName: topics) {
            size += sizeOfString(topicName, apiVersion);
            size += errorCodeSize;
            size += 16; // Topic ID
            size += specialFieldsSize;

            // --- Partitions ---
            List<PartitionData> partitions = partitionsMap.get(topicName);
            int partitionArrayLength = (partitions == null) ? 0 : partitions.size();
            size += sizeOfUnsignedVarInt(partitionArrayLength);

            if (partitionArrayLength > 0) {
                for (PartitionData pd : partitions) {
                    size += errorCodeSize;
                    size += 4 + 4 + 4; // partitionIndex + leader + leaderEpoch

                    int replicasSize = pd.replicas() != null ? pd.replicas().size() : 0;
                    size += sizeOfUnsignedVarInt(replicasSize + 1);
                    size += 4 * replicasSize;

                    int isrSize = pd.inSyncReplicas() != null ? pd.inSyncReplicas().size() : 0;
                    size += sizeOfUnsignedVarInt(isrSize + 1);
                    size += 4 * isrSize;

                    size += sizeOfUnsignedVarInt(1); // ELR
                    size += sizeOfUnsignedVarInt(1); // Last Known ELR
                    size += sizeOfUnsignedVarInt(1); // Offline
                    size += sizeOfUnsignedVarInt(0); // tagged fields
                }
            }
            size += sizeOfUnsignedVarInt(0); // Topic tagged fields
        }
        size += sizeOfUnsignedVarInt(0); // Cursor
        size += sizeOfUnsignedVarInt(0); // Top-level tagged fields
        return size;
    }

    private void writeString(ByteBuffer buf, String s, short apiVersion) {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        if (apiVersion >= 1) {
            writeUnsignedVarInt(data.length + 1, buf);
        } else {
            buf.putShort((short) data.length);
        }
        buf.put(data);
    }

    private void writeBytes(ByteBuffer buf, byte[] bytes) {
        buf.put(bytes);
    }

    private int sizeOfString(String s, short apiVersion) {
        int stringLength = s.getBytes(StandardCharsets.UTF_8).length;
        int lengthPrefixSize;
        if (apiVersion >= 1) {
            lengthPrefixSize = sizeOfUnsignedVarInt(stringLength + 1);
        } else {
            lengthPrefixSize = 2; // Rigid versions use a short (2 bytes)
        }
        return lengthPrefixSize + stringLength;
    }

    // =====================================================================================
    // HELPER METHODS for Logging
    // =====================================================================================

    /**
     * A helper to wrap a write operation, logging its details to the console.
     * @param buf The buffer being written to.
     * @param description A description of the field being written.
     * @param value The original decimal value of the data.
     * @param writer A lambda function that performs the actual write operation.
     */
    private void logWrite(ByteBuffer buf, String description, Object value, Runnable writer) {
        int startPos = buf.position();
        writer.run();
        int endPos = buf.position();

        byte[] writtenBytes = new byte[endPos - startPos];
        buf.position(startPos); // Go back to read the bytes
        buf.get(writtenBytes);
        buf.position(endPos);   // Restore buffer position

        String hexValue = bytesToHex(writtenBytes);
        String valueStr = "null";
        if(value != null) {
            valueStr = value.toString();
            if (value instanceof byte[]) {
                valueStr = "byte[" + ((byte[])value).length + "]"; // Don't print large byte arrays
            }
        }

        System.out.printf("[WRITE] %-25s | pos: %-3d | hex: %-20s | dec: %s%n",
                description, endPos, hexValue, valueStr);
    }

    /**
     * Converts a byte array to a hexadecimal string representation.
     * @param bytes The byte array to convert.
     * @return A string of hex values.
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    private static byte[] uuidToBytes(String uuidString) {
        if (uuidString == null) {
            return new byte[16];
        }

        UUID uuid = UUID.fromString(uuidString);
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

}

