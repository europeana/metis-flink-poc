package eu.europeana.cloud.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ExecutionRecord {

    private ExecutionRecordKey executionRecordKey;
    private String executionName;
    private String recordData;
}

