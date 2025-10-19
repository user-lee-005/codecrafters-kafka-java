package dto;

import java.util.List;

public class RecordBatch {
    private long baseOffset;
    private int batchLength;
    private int partitionLeaderEpoch;
    private byte magicByte;
    private int crc;
    private short attributes;
    private int lastOffsetData;
    private long baseTimeStamp;
    private long maxTimeStamp;
    private long producerId;
    private short producerEpoch;
    private int baseSequence;
    private int recordsLength;

    public long getBaseOffset() {
        return baseOffset;
    }

    public void setBaseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public int getBatchLength() {
        return batchLength;
    }

    public void setBatchLength(int batchLength) {
        this.batchLength = batchLength;
    }

    public int getPartitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public void setPartitionLeaderEpoch(int partitionLeaderEpoch) {
        this.partitionLeaderEpoch = partitionLeaderEpoch;
    }

    public byte getMagicByte() {
        return magicByte;
    }

    public void setMagicByte(byte magicByte) {
        this.magicByte = magicByte;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public short getAttributes() {
        return attributes;
    }

    public void setAttributes(short attributes) {
        this.attributes = attributes;
    }

    public int getLastOffsetData() {
        return lastOffsetData;
    }

    public void setLastOffsetData(int lastOffsetData) {
        this.lastOffsetData = lastOffsetData;
    }

    public long getBaseTimeStamp() {
        return baseTimeStamp;
    }

    public void setBaseTimeStamp(long baseTimeStamp) {
        this.baseTimeStamp = baseTimeStamp;
    }

    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }

    public void setMaxTimeStamp(long maxTimeStamp) {
        this.maxTimeStamp = maxTimeStamp;
    }

    public long getProducerId() {
        return producerId;
    }

    public void setProducerId(long producerId) {
        this.producerId = producerId;
    }

    public short getProducerEpoch() {
        return producerEpoch;
    }

    public void setProducerEpoch(short producerEpoch) {
        this.producerEpoch = producerEpoch;
    }

    public int getBaseSequence() {
        return baseSequence;
    }

    public void setBaseSequence(int baseSequence) {
        this.baseSequence = baseSequence;
    }

    public int getRecordsLength() {
        return recordsLength;
    }

    public void setRecordsLength(int recordsLength) {
        this.recordsLength = recordsLength;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    private List<Record> records;

    public List<Record> getRecords() {
        return records;
    }

    public void setRecord(List<Record> records) {
        this.records = records;
    }
}
