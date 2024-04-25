package eu.europeana.cloud.flink.validation;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.EXECUTION_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PREVIOUS_STEP_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.ROOT_LOCATION;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SCHEMATRON_LOCATION;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SCHEMA_NAME;
import static eu.europeana.cloud.flink.common.utils.JobUtils.useNewIfNull;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.JobParameters;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class ValidationJob extends AbstractFollowingJob<ValidationTaskParams> {

  public static void main(String[] args) throws Exception {
    ValidationJob validation = new ValidationJob();
    validation.executeJob(args);
  }

  @Override
  protected String mainOperatorName() {
    return "Validate";
  }

  @Override
  protected FollowingJobMainOperator createMainOperator(Properties properties, ValidationTaskParams taskParams) {
    return new ValidationOperator(taskParams);
  }

  @Override
  protected JobParameters<ValidationTaskParams> prepareParameters(String[] args) {
    ParameterTool tool = ParameterTool.fromArgs(args);
    String datasetId = tool.getRequired(DATASET_ID);
    ValidationTaskParams taskParams = ValidationTaskParams
        .builder()
        .datasetId(datasetId)
        .executionId(useNewIfNull(tool.get(EXECUTION_ID)))
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .schemaName(tool.getRequired(SCHEMA_NAME))
        .rootLocation(tool.getRequired(ROOT_LOCATION))
        .schematronLocation(tool.get(SCHEMATRON_LOCATION))
        .build();
    return new JobParameters<>(tool, taskParams);
  }
}
