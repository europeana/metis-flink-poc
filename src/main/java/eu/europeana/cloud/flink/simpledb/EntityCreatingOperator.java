package eu.europeana.cloud.flink.simpledb;

import eu.europeana.cloud.flink.oai.OAITaskInformation;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.MapFunction;

public class EntityCreatingOperator implements MapFunction<RecordTuple, RecordExecutionEntity> {

  private final String jobName;
  private OAITaskInformation taskInformation;

  public EntityCreatingOperator(String jobName, OAITaskInformation taskInformation) {
    this.jobName = jobName;
    this.taskInformation = taskInformation;
  }

  @Override
  public RecordExecutionEntity map(RecordTuple record) throws Exception {
    return RecordExecutionEntity.builder()
                                .datasetId(taskInformation.getDatasetId())
                                .executionId(taskInformation.getExecutionId().toString())
                                .executionName(jobName)
                                .recordId(record.getEuropeanaId())
                                .recordData(new String(record.getFileContent(), StandardCharsets.UTF_8))
                                .build();
  }
}
