package processors;

import dto.*;
import dto.Record;
import utils.RecordType;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.*;
import java.nio.file.*;
import java.time.LocalDateTime;

public class ClusterMetadataProcessor {

    /**
     * Downloads the Kafka cluster metadata file from a given source path or URL
     * into the test resources folder or a local temp directory.
     *
     * @param sourceUrlOrPath Remote URL (http, https, ftp, file) or local path.
     * @param targetDir Local directory to save (e.g., "src/test/resources/metadata")
     * @return Path of the downloaded file
     * @throws IOException if download or copy fails
     */
    public Path downloadClusterMetadata(String sourceUrlOrPath, String targetDir) throws IOException {
        Path targetDirPath = Paths.get(targetDir);
        if (!Files.exists(targetDirPath)) {
            Files.createDirectories(targetDirPath);
        }

        // Name file with timestamp to avoid overwrites
        String fileName = "cluster_metadata_" + LocalDateTime.now().toString().replace(":", "-") + ".log";
        Path targetFile = targetDirPath.resolve(fileName);

        if (sourceUrlOrPath.startsWith("http")) {
            // HTTP(S) download
            URL url = new URL(sourceUrlOrPath);
            try (InputStream in = url.openStream()) {
                Files.copy(in, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            // Local file copy
            Path sourcePath = Paths.get(sourceUrlOrPath);
            if (!Files.exists(sourcePath)) {
                throw new FileNotFoundException("Metadata file not found: " + sourcePath);
            }
            Files.copy(sourcePath, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }

        System.out.println("✅ Metadata file downloaded to: " + targetFile.toAbsolutePath());
        return targetFile;
    }

    public TopicClusterMetadata loadMetadata(String filePath) {
        System.out.println("Loading cluster metadata from: " + filePath);
        try(DataInputStream dataInputStream = new DataInputStream(new FileInputStream(filePath))) {
            List<RecordBatch> recordBatches = new ArrayList<>();

            while (dataInputStream.available() > 0) {
                long baseOffset;
                int batchLength;
                try {
                    baseOffset = dataInputStream.readLong();
                    batchLength = dataInputStream.readInt();
                } catch (EOFException e) {
                    System.out.println("Reached end of metadata file.");
                    break;
                }
                if (batchLength <= 0) {
                    System.out.println("Invalid batch length (" + batchLength + "), skipping.");
                    continue;
                }
                byte[] batchBytes = new byte[batchLength];
                dataInputStream.readFully(batchBytes);
                try (DataInputStream batchStream = new DataInputStream(new ByteArrayInputStream(batchBytes))) {
                    RecordBatch recordBatch = getRecordBatch(batchStream);
                    recordBatch.setBaseOffset(baseOffset);
                    recordBatch.setBatchLength(batchLength);
                    int recordsCount = recordBatch.getRecordsLength();
                    if (recordsCount < 0) {
                        System.out.println("[DEBUG::loadMetadata] Batch has " + recordsCount + " records, skipping record read.");
                    } else {
                        List<Record> records = new ArrayList<>();
                        while(recordsCount > 0) {
                            Record record = getRecord(batchStream);
                            records.add(record);
                            recordsCount --;
                        }
                        recordBatch.setRecords(records);
                    }
                    recordBatches.add(recordBatch);
                }
            }

            System.out.println("Successfully loaded cluster metadata.");
            return new TopicClusterMetadata(recordBatches);
        } catch (FileNotFoundException e) {
            System.err.println("FATAL: Metadata file not found! " + filePath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("FATAL: Failed to read metadata file.");
            throw new RuntimeException(e);
        }
    }

    public MetadataCache parseMetadata(TopicClusterMetadata metadata) {
        Map<String, String> topicUuidToName = new HashMap<>();
        List<Record> partitionRecords = new ArrayList<>();

        // 1. First Pass: Get all topic names and partition records
        for (RecordBatch batch : metadata.getRecordBatches()) {
            for (Record record : batch.getRecords()) {
                if (record.getType() == RecordType.TOPIC_RECORD.getType()) {
                    topicUuidToName.put(
                            record.getTopicUuid(),
                            record.getTopicName()
                    );
                } else if (record.getType() == RecordType.PARTITION_RECORD.getType()) {
                    partitionRecords.add(record);
                }
            }
        }

        // 2. Second Pass: Build the partition maps
        Map<String, List<PartitionData>> byName = new HashMap<>();
        Map<String, List<PartitionData>> byUuid = new HashMap<>();

        for (Record record : partitionRecords) {
            String topicUuid = record.getTopicUuid();
            String topicName = topicUuidToName.get(topicUuid);

            if (topicName == null) {
                continue;
            }

            PartitionData pData = getPartitionData(record, topicUuid);

            byName.computeIfAbsent(topicName, k -> new ArrayList<>()).add(pData);
            System.out.println("By Name :: " + byName);
            byUuid.computeIfAbsent(topicUuid, k -> new ArrayList<>()).add(pData);
            System.out.println("By UUID :: " + byUuid);
        }

        return new MetadataCache(byName, byUuid);
    }

    private static PartitionData getPartitionData(Record record, String topicUuid) {
        System.out.println("Record to partition data :: " + record.toString());
        List<Integer> replicas = (record.getReplicaArrayLength() > 0)
                ? List.of(record.getReplicaId())
                : List.of();
        List<Integer> isr = (record.getSyncReplicaArrayLength() > 0)
                ? List.of(record.getInSyncReplicaIds())
                : List.of();

        PartitionData pData = new PartitionData(
                record.getPartitionId(),
                topicUuid,
                replicas,
                isr,
                record.getLeader(),
                record.getLeaderEpoch(),
                record.getPartitionEpoch()
        );
        System.out.println("Partition Data :: " + pData);
        return pData;
    }

    private Record getRecord(DataInputStream dataInputStream) throws IOException {
        System.out.println("[DEBUG::getRecord] --- Reading New Record ---");
        Record record = new Record();

        System.out.println("[DEBUG::getRecord] Reading Length...");
        record.setLength(readVarInt(dataInputStream));
        System.out.println("[DEBUG::getRecord] - Length: " + record.getLength());

        System.out.println("[DEBUG::getRecord] Reading Attributes (byte)...");
        record.setAttributes(dataInputStream.readByte());
        System.out.println("[DEBUG::getRecord] - Attributes: " + record.getAttributes());

        System.out.println("[DEBUG::getRecord] Reading TimeStampDelta (varint)...");
        record.setTimeStampDelta(readVarInt(dataInputStream));
        System.out.println("[DEBUG::getRecord] - TimeStampDelta: " + record.getTimeStampDelta());

        System.out.println("[DEBUG::getRecord] Reading OffsetData (varint)...");
        record.setOffsetData(readVarInt(dataInputStream));
        System.out.println("[DEBUG::getRecord] - OffsetData: " + record.getOffsetData());

        System.out.println("[DEBUG::getRecord] Reading KeyLength...");
        record.setKeyLength(readVarInt(dataInputStream));
        System.out.println("[DEBUG::getRecord] - KeyLength: " + record.getKeyLength());

        if (record.getKeyLength() > 0) {
            System.out.println("[DEBUG::getRecord] Reading Key (" + record.getKeyLength() + " bytes)...");
            record.setKey(dataInputStream.readNBytes(record.getKeyLength()));
            System.out.println("[DEBUG::getRecord] - Key: " + new String(record.getKey(), StandardCharsets.UTF_8));
        } else {
            System.out.println("[DEBUG::getRecord] - KeyLength is 0, skipping key read.");
        }

        System.out.println("[DEBUG::getRecord] Reading ValueLength...");
        record.setValueLength(readVarInt(dataInputStream));
        System.out.println("[DEBUG::getRecord] - ValueLength: " + record.getValueLength());

        if (record.getValueLength() > 0) {
            System.out.println("[DEBUG::getRecord] Reading Value (" + record.getValueLength() + " bytes)...");
            byte[] valueBytes = dataInputStream.readNBytes(record.getValueLength());
            DataInputStream valueStream = new DataInputStream(new ByteArrayInputStream(valueBytes));

            record.setFrameVersion(valueStream.readByte());
            System.out.println("[DEBUG::getRecord]   - Value.FrameVersion: " + record.getFrameVersion());

            record.setType(valueStream.readByte());
            System.out.println("[DEBUG::getRecord]   - Value.Type: " + record.getType());

            record.setVersion(valueStream.readByte());
            System.out.println("[DEBUG::getRecord]   - Value.Version: " + record.getVersion());

            // This switch will now get the *correct* type
            RecordType type = RecordType.fromType(record.getType());
            System.out.println("[DEBUG::getRecord]   - Value.RecordType: " + type);
            switch (type) {
                case FEATURE_LEVEL_RECORD:
                    record.setNameLength(valueStream.readUnsignedByte());
                    System.out.println("[DEBUG::getRecord]   - Value.NameLength: " + record.getNameLength());
                    record.setName(readString(valueStream, record.getNameLength() - 1));
                    System.out.println("[DEBUG::getRecord]   - Value.Name: " + record.getName());
                    record.setFeatureLevel(valueStream.readShort());
                    System.out.println("[DEBUG::getRecord]   - Value.FeatureLevel: " + record.getFeatureLevel());
                    break;

                case TOPIC_RECORD:
                    record.setNameLength(valueStream.readUnsignedByte());
                    System.out.println("[DEBUG::getRecord]   - Value.NameLength: " + record.getNameLength());
                    record.setTopicName(readString(valueStream, record.getNameLength() - 1));
                    System.out.println("[DEBUG::getRecord]   - Value.TopicName: " + record.getTopicName());
                    record.setTopicUuid(readUUID(valueStream).toString());
                    System.out.println("[DEBUG::getRecord]   - Value.TopicUUID: " + record.getTopicUuid());
                    break;

                case PARTITION_RECORD:
                    record.setPartitionId(valueStream.readInt());
                    System.out.println("[DEBUG::getRecord]   - Value.PartitionId: " + record.getPartitionId());
                    record.setTopicUuid(readUUID(valueStream).toString());
                    System.out.println("[DEBUG::getRecord]   - Value.TopicUUID: " + record.getTopicUuid());
                    record.setReplicaArrayLength(valueStream.readUnsignedByte());
                    System.out.println("[DEBUG::getRecord]   - Value.ReplicaArrayLength: " + record.getReplicaArrayLength());
                    if (record.getReplicaArrayLength() > 0) {
                        record.setReplicaId(valueStream.readInt());
                        System.out.println("[DEBUG::getRecord]   - Value.ReplicaId: " + record.getReplicaId());
                    }
                    record.setSyncReplicaArrayLength(valueStream.readUnsignedByte());
                    System.out.println("[DEBUG::getRecord]   - Value.SyncReplicaArrayLength: " + record.getSyncReplicaArrayLength());
                    if (record.getSyncReplicaArrayLength() > 0) {
                        record.setInSyncReplicaIds(valueStream.readInt());
                        System.out.println("[DEBUG::getRecord]   - Value.InSyncReplicaIds: " + record.getInSyncReplicaIds());
                    }
                    record.setRemovingReplicaArrayLength(valueStream.readUnsignedByte());
                    record.setAddingReplicaArrayLength(valueStream.readUnsignedByte());
                    record.setLeader(valueStream.readInt());
                    System.out.println("[DEBUG::getRecord]   - Value.Leader: " + record.getLeader());
                    record.setLeaderEpoch(valueStream.readInt());
                    System.out.println("[DEBUG::getRecord]   - Value.LeaderEpoch: " + record.getLeaderEpoch());
                    record.setPartitionEpoch(valueStream.readInt());
                    System.out.println("[DEBUG::getRecord]   - Value.PartitionEpoch: " + record.getPartitionEpoch());
                    record.setDirArrayLength(valueStream.readUnsignedByte());
                    System.out.println("[DEBUG::getRecord]   - Value.DirArrayLength: " + record.getDirArrayLength());
                    record.setDirUuid(readUUID(valueStream).toString());
                    System.out.println("[DEBUG::getRecord]   - Value.DirArrayUUID: " + record.getDirUuid());
                    break;

                default:
                    System.out.println("[DEBUG::getRecord]   - Unknown record type, skipping value parse.");
                    break;
            }
            valueStream.close();
        } else {
            System.out.println("[DEBUG::getRecord] - ValueLength is 0, skipping value read.");
        }

        System.out.println("[DEBUG::getRecord] Reading TaggedFieldsCount (ubyte)...");
        record.setTaggedFieldsCount(dataInputStream.readUnsignedByte());
        System.out.println("[DEBUG::getRecord] - TaggedFieldsCount: " + record.getTaggedFieldsCount());

//        System.out.println("[DEBUG::getRecord] Reading HeadersArrayCount (ubyte)...");
//        record.setHeadersArrayCount(dataInputStream.readUnsignedByte());
//        System.out.println("[DEBUG::getRecord] - HeadersArrayCount: " + record.getHeadersArrayCount());

        System.out.println("[DEBUG::getRecord] --- Finished Record ---");
        return record;
    }

    private RecordBatch getRecordBatch(DataInputStream dataInputStream) throws IOException {
        System.out.println("\n[DEBUG::getRecordBatch] === Reading New Record Batch ===");
        RecordBatch recordBatch = new RecordBatch();

//        long baseOffset = dataInputStream.readLong();
//        recordBatch.setBaseOffset(baseOffset);
//        System.out.println("[DEBUG::getRecordBatch] - BaseOffset: " + baseOffset);
//
//        int batchLength = dataInputStream.readInt();
//        recordBatch.setBatchLength(batchLength);
//        System.out.println("[DEBUG::getRecordBatch] - BatchLength: " + batchLength);

        int leaderEpoch = dataInputStream.readInt();
        recordBatch.setPartitionLeaderEpoch(leaderEpoch);
        System.out.println("[DEBUG::getRecordBatch] - PartitionLeaderEpoch: " + leaderEpoch);

        byte magicByte = dataInputStream.readByte();
        recordBatch.setMagicByte(magicByte);
        System.out.println("[DEBUG::getRecordBatch] - MagicByte: " + magicByte);

        int crc = dataInputStream.readInt();
        recordBatch.setCrc(crc);
        System.out.println("[DEBUG::getRecordBatch] - CRC: " + crc);

        short attributes = dataInputStream.readShort();
        recordBatch.setAttributes(attributes);
        System.out.println("[DEBUG::getRecordBatch] - Attributes: " + attributes);

        int lastOffsetData = dataInputStream.readInt();
        recordBatch.setLastOffsetData(lastOffsetData);
        System.out.println("[DEBUG::getRecordBatch] - LastOffsetData: " + lastOffsetData);

        long baseTimeStamp = dataInputStream.readLong();
        recordBatch.setBaseTimeStamp(baseTimeStamp);
        System.out.println("[DEBUG::getRecordBatch] - BaseTimeStamp: " + baseTimeStamp);

        long maxTimeStamp = dataInputStream.readLong();
        recordBatch.setMaxTimeStamp(maxTimeStamp);
        System.out.println("[DEBUG::getRecordBatch] - MaxTimeStamp: " + maxTimeStamp);

        long producerId = dataInputStream.readLong();
        recordBatch.setProducerId(producerId);
        System.out.println("[DEBUG::getRecordBatch] - ProducerId: " + producerId);

        short producerEpoch = dataInputStream.readShort();
        recordBatch.setProducerEpoch(producerEpoch);
        System.out.println("[DEBUG::getRecordBatch] - ProducerEpoch: " + producerEpoch);

        int baseSequence = dataInputStream.readInt();
        recordBatch.setBaseSequence(baseSequence);
        System.out.println("[DEBUG::getRecordBatch] - RecordsLength (Count): " + baseSequence);

        int recordsLength = dataInputStream.readInt();
        recordBatch.setRecordsLength(recordsLength);
        System.out.println("[DEBUG::getRecordBatch] - RecordsLength (Count): " + recordsLength);

        System.out.println("[DEBUG::getRecordBatch] === Finished Record Batch Header ===\n");
        return recordBatch;
    }

    private String readString(DataInputStream dataInputStream, int length) throws IOException {
        if (length <= 0) {
            return "";
        }
        byte[] buffer = new byte[length];
        int bytesRead = dataInputStream.read(buffer);
        if (bytesRead != length) {
            throw new IOException("Expected " + length + " bytes, but only read " + bytesRead);
        }
        return new String(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Reads a signed varint from the input stream.
     * USES ZigZag decoding.
     * Used for timestamps, offsets, etc.
     */
    private int readVarInt(DataInputStream input) throws IOException {
        int value = 0;
        int position = 0;
        while (true) {
            int b = input.readByte();
            value |= (b & 0x7F) << position;
            if ((b & 0x80) == 0) break;
            position += 7;
            if (position > 28) throw new IOException("Varint too long (signed)");
        }
        // Zigzag decode
        return (value >>> 1) ^ -(value & 1);
    }

    private UUID readUUID(DataInputStream dataInputStream) throws IOException {
        byte[] buffer = new byte[16];
        int bytesRead = dataInputStream.read(buffer);
        if (bytesRead != 16) {
            throw new IOException("Expected 16 bytes for UUID, but got " + bytesRead);
        }

        ByteBuffer bb = ByteBuffer.wrap(buffer);
        long mostSigBits = bb.getLong();
        long leastSigBits = bb.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

}