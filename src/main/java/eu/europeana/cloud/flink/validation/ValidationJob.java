package eu.europeana.cloud.flink.validation;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.*;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class ValidationJob extends AbstractFollowingJob<ValidationTaskParams> {

  public ValidationJob(Properties properties, ValidationTaskParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected FollowingJobMainOperator createMainOperator(Properties properties, ValidationTaskParams taskParams) {
    return new ValidationOperator(taskParams);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired(DATASET_ID);
    ValidationTaskParams taskParams = ValidationTaskParams
        .builder()
        .datasetId(metisDatasetId)
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .schemaName(tool.getRequired(SCHEMA_NAME))
        .rootLocation(tool.getRequired(ROOT_LOCATION))
        .schematronLocation(tool.get(SCHEMATRON_LOCATION))
        .build();
    ValidationJob job = new ValidationJob(readProperties(tool.getRequired(CONFIGURATION_FILE_PATH)), taskParams);
    job.execute();
  }


}
