package eu.europeana.cloud.flink.simpledb;

import eu.europeana.cloud.flink.common.TaskParams;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.MapFunction;

public class DbEntityCreatingOperator implements MapFunction<RecordTuple, RecordExecutionEntity> {

  private final String jobName;
  private TaskParams taskParams;

  public DbEntityCreatingOperator(String jobName, TaskParams taskParams) {
    this.jobName = jobName;
    this.taskParams = taskParams;
  }

  @Override
  public RecordExecutionEntity map(RecordTuple record) throws Exception {
    return RecordExecutionEntity.builder()
                                .datasetId(taskParams.getDatasetId())
                                .executionId(taskParams.getExecutionId().toString())
                                .executionName(jobName)
                                .recordId(record.getRecordId())
                                .recordData(new String(record.getFileContent(), StandardCharsets.UTF_8))
                                .build();
  }
}
