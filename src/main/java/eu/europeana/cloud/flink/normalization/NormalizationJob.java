package eu.europeana.cloud.flink.normalization;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingTaskParams;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalizationJob extends AbstractFollowingJob<FollowingTaskParams> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationJob.class);

  public NormalizationJob(Properties properties, FollowingTaskParams taskParams) throws Exception {
    super(properties,taskParams);
  }

  @Override
  protected NormalizationOperator createMainOperator(FollowingTaskParams taskParams) {
    return new NormalizationOperator();
  }


  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    FollowingTaskParams taskParams = FollowingTaskParams
        .builder()
        .datasetId(tool.getRequired("datasetId"))
        .previousStepId(UUID.fromString(tool.getRequired("previousStepId")))
        .build();

    NormalizationJob job = new NormalizationJob(readProperties(tool.getRequired("configurationFilePath")), taskParams);
    job.execute();
  }


}
