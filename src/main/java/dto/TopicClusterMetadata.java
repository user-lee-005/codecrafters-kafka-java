package dto;

import java.util.List;

public class TopicClusterMetadata {
    List<RecordBatch> recordBatches;

    public TopicClusterMetadata(List<RecordBatch> recordBatches) {
        this.recordBatches = recordBatches;
    }

    public List<RecordBatch> getRecordBatches() {
        return recordBatches;
    }

    public void setRecordBatches(List<RecordBatch> recordBatches) {
        this.recordBatches = recordBatches;
    }
}
