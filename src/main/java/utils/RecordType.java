package utils;

public enum RecordType {
    FEATURE_LEVEL_RECORD(12),
    TOPIC_RECORD(2),
    PARTITION_RECORD(3);

    private final int type;

    RecordType(int type) {
        this.type = type;
    }

    public static RecordType fromType(int type) {
        for(RecordType recordType: RecordType.values()) {
            if(recordType.type == type) {
                return recordType;
            }
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    public int getType() {
        return this.type;
    }
}
