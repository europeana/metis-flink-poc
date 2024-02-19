package eu.europeana.cloud.flink.validation;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationJob extends AbstractFollowingJob<ValidationTaskParams> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationJob.class);

  public ValidationJob(Properties properties, ValidationTaskParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected FollowingJobMainOperator createMainOperator(ValidationTaskParams taskParams) {
    return new ValidationOperator(taskParams);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired("datasetId");
    ValidationTaskParams taskParams = ValidationTaskParams
        .builder()
        .datasetId(metisDatasetId)
        .previousStepId(UUID.fromString(tool.getRequired("previousStepId")))
        .schemaName(tool.getRequired("schemaName"))
        .rootLocation(tool.getRequired("rootLocation"))
        .schematronLocation(tool.get("schematronLocation"))
        .build();
    ValidationJob job = new ValidationJob(readProperties(tool.getRequired("configurationFilePath")), taskParams);
    job.execute();
  }


}
