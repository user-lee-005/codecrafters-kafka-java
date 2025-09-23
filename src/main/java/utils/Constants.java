package utils;

public class Constants {
    public static final int INT_32_LENGTH = 4;
    public static final int INT_16_LENGTH = 2;

    public static final int messageSize = INT_32_LENGTH;
    public static final int reqApiKeySize = INT_16_LENGTH;
    public static final int reqApiVersionSize = INT_16_LENGTH;
    public static final int correlationIdSize = INT_32_LENGTH;
    public static final int errorCodeSize = INT_16_LENGTH;
    public static final int throttleTimeMsSize = INT_32_LENGTH;


    public static final short unsupportedVersionErrorCode = 35;
    public static final short UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE = 3;

}
