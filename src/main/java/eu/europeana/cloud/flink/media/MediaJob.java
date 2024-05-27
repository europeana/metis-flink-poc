package eu.europeana.cloud.flink.media;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.EXECUTION_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PARALLELISM;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PREVIOUS_STEP_ID;
import static eu.europeana.cloud.flink.common.utils.JobUtils.useNewIfNull;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingTaskParams;
import eu.europeana.cloud.flink.common.JobParameters;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class MediaJob extends AbstractFollowingJob<FollowingTaskParams> {

  public static void main(String[] args) throws Exception {
    MediaJob job = new MediaJob();
    job.executeJob(args);
  }

  protected String mainOperatorName() {
    return "Extract media";
  }

  @Override
  protected MediaOperator createMainOperator(Properties properties, FollowingTaskParams taskParams) {
    return new MediaOperator();
  }

  @Override
  protected JobParameters<FollowingTaskParams> prepareParameters(String[] args) {
    ParameterTool tool = ParameterTool.fromArgs(args);
    FollowingTaskParams taskParams = FollowingTaskParams
        .builder()
        .datasetId(tool.getRequired(DATASET_ID))
        .executionId(useNewIfNull(tool.get(EXECUTION_ID)))
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .parallelism(tool.getInt(PARALLELISM, 1))
        .build();
    return new JobParameters<>(tool, taskParams);
  }
}
