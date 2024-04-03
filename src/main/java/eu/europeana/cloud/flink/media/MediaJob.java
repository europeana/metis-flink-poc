package eu.europeana.cloud.flink.media;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.*;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingTaskParams;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class MediaJob extends AbstractFollowingJob<FollowingTaskParams> {

  public MediaJob(Properties properties, FollowingTaskParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected MediaOperator createMainOperator(Properties properties, FollowingTaskParams taskParams) {
    return new MediaOperator();
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    FollowingTaskParams taskParams = FollowingTaskParams
        .builder()
        .datasetId(tool.getRequired(DATASET_ID))
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .build();

    MediaJob job = new MediaJob(readProperties(tool.getRequired(CONFIGURATION_FILE_PATH)), taskParams);
    job.execute();
  }

}
