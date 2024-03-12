package eu.europeana.cloud.flink.simpledb;

import eu.europeana.cloud.flink.common.TaskParams;
import eu.europeana.cloud.flink.common.tuples.ErrorTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class DbErrorEntityCreatingOperator implements MapFunction<ErrorTuple, RecordExecutionExceptionLogEntity> {

  private final String jobName;
  private TaskParams taskParams;

  public DbErrorEntityCreatingOperator(String jobName, TaskParams taskParams) {
    this.jobName = jobName;
    this.taskParams = taskParams;
  }

  @Override
  public RecordExecutionExceptionLogEntity map(ErrorTuple recordError) throws Exception {
    return RecordExecutionExceptionLogEntity.builder()
                                            .datasetId(taskParams.getDatasetId())
                                            .executionId(taskParams.getExecutionId().toString())
                                            .executionName(jobName)
                                            .recordId(recordError.getRecordId())
                                            .exception(recordError.getException())
                                            .build();
  }
}
