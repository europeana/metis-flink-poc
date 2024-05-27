package eu.europeana.cloud.flink.workflow;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.CONFIGURATION_FILE_PATH;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METADATA_PREFIX;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.OAI_REPOSITORY_URL;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PARALLELISM;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SET_SPEC;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.cloud.flink.workflow.entities.SubmitJobRequest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowExecutorWithAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowExecutorWithAPI.class);

  private final String datasetId;
  private final OaiHarvest oaiHarvest;

  private JobExecutor jobExecutor;
  private int parallelism;

  public WorkflowExecutorWithAPI(Properties serverConfiguration, String datasetId, OaiHarvest oaiHarvest, int parallelism) {
    this.jobExecutor = new JobExecutor(serverConfiguration);
    this.datasetId = datasetId;
    this.oaiHarvest = oaiHarvest;
    this.parallelism = parallelism;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    ParameterTool tool = ParameterTool.fromArgs(args);
    OaiHarvest oaiHarvest = new OaiHarvest(
        tool.getRequired(OAI_REPOSITORY_URL),
        tool.getRequired(METADATA_PREFIX),
        tool.getRequired(SET_SPEC));
    String datasetId = tool.getRequired(DATASET_ID);
    Properties serverConfiguration = readProperties(tool.getRequired(CONFIGURATION_FILE_PATH));
    int parallelism = tool.getInt(PARALLELISM, 1);
    new WorkflowExecutorWithAPI(serverConfiguration, datasetId, oaiHarvest, parallelism).execute();
  }

  private void execute() {
    try {
      LOGGER.info("Starting workflow.");
      UUID executionId = UUIDs.timeBased();
      SubmitJobRequest oaiRequest = StepFactories.createOAIRequest(datasetId, executionId, oaiHarvest, parallelism);
      jobExecutor.execute(oaiRequest);
      for (StepFactories.FollowingJobRequestFactory factory : StepFactories.LIST) {
        UUID previousExecutionId = executionId;
        executionId = UUIDs.timeBased();
        SubmitJobRequest currentStep = factory.createRequest(datasetId, executionId, previousExecutionId, parallelism);
        jobExecutor.execute(currentStep);
      }
      LOGGER.info("Completed workflow.");
    } catch (Exception e) {
      LOGGER.error("Workflow ended with an error!", e);
    }
  }

}
