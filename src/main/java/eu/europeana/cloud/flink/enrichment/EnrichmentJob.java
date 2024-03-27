package eu.europeana.cloud.flink.enrichment;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.*;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingTaskParams;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class EnrichmentJob extends AbstractFollowingJob<FollowingTaskParams> {

  public EnrichmentJob(Properties properties, FollowingTaskParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected EnrichmentOperator createMainOperator(Properties properties, FollowingTaskParams taskParams) {
    return new EnrichmentOperator(properties);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    FollowingTaskParams taskParams = FollowingTaskParams
        .builder()
        .datasetId(tool.getRequired(DATASET_ID))
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .build();

    EnrichmentJob job = new EnrichmentJob(readProperties(tool.getRequired(CONFIGURATION_FILE_PATH)), taskParams);
    job.execute();
  }

}
