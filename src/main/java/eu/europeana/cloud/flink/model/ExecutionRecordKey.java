package eu.europeana.cloud.flink.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ExecutionRecordKey implements Serializable {
    private String datasetId;
    private String executionId;
    private String recordId;
}
