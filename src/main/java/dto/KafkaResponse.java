package dto;

import java.util.List;

public class KafkaResponse {
    private int correlationId;
    private short errorCode;
    private List<ApiVersionDTO> apiVersionDTOList;
    private int throttleTimeMs;

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public List<ApiVersionDTO> getApiVersionDTOList() {
        return apiVersionDTOList;
    }

    public void setApiVersionDTOList(List<ApiVersionDTO> apiVersionDTOList) {
        this.apiVersionDTOList = apiVersionDTOList;
    }

    public int getThrottleTimeMs() {
        return throttleTimeMs;
    }

    public void setThrottleTimeMs(int throttleTimeMs) {
        this.throttleTimeMs = throttleTimeMs;
    }

    public KafkaResponse(int correlationId, short errorCode, List<ApiVersionDTO> apiVersionDTOList, int throttleTimeMs) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.apiVersionDTOList = apiVersionDTOList;
        this.throttleTimeMs = throttleTimeMs;
    }

    public static class ApiVersionDTO {
        private short apiKey;
        private short minVersion;
        private short maxVersion;

        public ApiVersionDTO(short apiKey, short minVersion, short maxVersion) {
            this.apiKey = apiKey;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }

        public short getApiKey() {
            return apiKey;
        }

        public void setApiKey(short apiKey) {
            this.apiKey = apiKey;
        }

        public short getMinVersion() {
            return minVersion;
        }

        public void setMinVersion(short minVersion) {
            this.minVersion = minVersion;
        }

        public short getMaxVersion() {
            return maxVersion;
        }

        public void setMaxVersion(short maxVersion) {
            this.maxVersion = maxVersion;
        }
    }
}
