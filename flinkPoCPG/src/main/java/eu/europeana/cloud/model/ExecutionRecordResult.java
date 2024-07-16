package eu.europeana.cloud.model;

import lombok.Builder;
import lombok.Data;

/**
 * Keeps the result of the execution of single record.
 */
@Builder
@Data
public class ExecutionRecordResult {

    private ExecutionRecord executionRecord;
    private String exception;

    public void setRecordData(String recordData) {
        executionRecord.setRecordData(recordData);
    }

    public String getRecordData() {
        return executionRecord.getRecordData();
    }

    public String getRecordId() {
        return executionRecord.getExecutionRecordKey().getRecordId();
    }

    public static ExecutionRecordResult from(ExecutionRecord executionRecord) {
        return ExecutionRecordResult
                .builder()
                .executionRecord(executionRecord)
                .build();
    }

    public static ExecutionRecordResult from(ExecutionRecord sourceRecord, long taskId, String stepName) {
        ExecutionRecordResult resultRecord = ExecutionRecordResult.from(sourceRecord);
        resultRecord.getExecutionRecord().getExecutionRecordKey().setExecutionId(taskId + "");
        resultRecord.getExecutionRecord().setExecutionName(stepName);
        return resultRecord;
    }

    public static ExecutionRecordResult from(
            ExecutionRecord executionRecord,
            String executionId,
            String executionName,
            String recordData,
            String exception

    ) {
        return ExecutionRecordResult
                .builder()
                .executionRecord(
                        ExecutionRecord.builder()
                                .executionRecordKey(
                                        ExecutionRecordKey.builder()
                                                .datasetId(executionRecord.getExecutionRecordKey().getDatasetId())
                                                .executionId(executionId)
                                                .recordId(executionRecord.getExecutionRecordKey().getRecordId())
                                                .build())
                                .executionName(executionName)
                                .recordData(recordData)
                                .build()
                )
                .exception(exception)
                .build();
    }
}
