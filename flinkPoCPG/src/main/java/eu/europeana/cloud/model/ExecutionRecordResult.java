package eu.europeana.cloud.model;

import lombok.Builder;
import lombok.Data;

/**
 * Keeps the result of the execution of single record.
 *
 */
@Builder
@Data
public class ExecutionRecordResult {

    private ExecutionRecord executionRecord;
    private String exception;

    public void setRecordData(String recordData){
        executionRecord.setRecordData(recordData);
    }
    public String getRecordData(){
        return executionRecord.getRecordData();
    }

    public String getRecordId(){
        return executionRecord.getExecutionRecordKey().getRecordId();
    }

    public static ExecutionRecordResult from(ExecutionRecord executionRecord){
        return ExecutionRecordResult
                .builder()
                .executionRecord(executionRecord)
                .build();
    }
}
