package eu.europeana.cloud.flink.workflow;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METADATA_PREFIX;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.OAI_REPOSITORY_URL;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PARALLELISM;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SET_SPEC;

import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.cloud.flink.common.GenericJob;
import eu.europeana.cloud.flink.common.GenericJobFactory;
import eu.europeana.cloud.flink.workflow.entities.SubmitJobRequest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import java.util.UUID;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowExecutorFullCycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowExecutorFullCycle.class);

  public static void main(String[] args) {
    new WorkflowExecutorFullCycle().execute(args);
  }

  private void execute(String[] args) {
    try {
      LOGGER.info("Starting workflow.");
      ParameterTool tool = ParameterTool.fromArgs(args);
      String datasetId = tool.getRequired(DATASET_ID);
      UUID executionId = UUIDs.timeBased();
      OaiHarvest oaiHarvest = new OaiHarvest(
          tool.getRequired(OAI_REPOSITORY_URL),
          tool.getRequired(METADATA_PREFIX),
          tool.getRequired(SET_SPEC));
      int parallelism = tool.getInt(PARALLELISM, 1);
      SubmitJobRequest oaiRequest = StepFactories.createOAIRequest(datasetId, executionId, oaiHarvest, parallelism);
      LOGGER.info("{} {}", oaiRequest.getEntryClass(), oaiRequest.getProgramArgs());
      GenericJob startStep = GenericJobFactory.createJob(oaiRequest.getEntryClass());

      String[] arguments = oaiRequest.getProgramArgs().split(" ");
      JobExecutionResult jobExecutionResult = startStep.executeJob(arguments);
      LOGGER.info("result OAI {}", jobExecutionResult);
      for (StepFactories.FollowingJobRequestFactory factory : StepFactories.LIST) {
        UUID previousExecutionId = executionId;
        executionId = UUIDs.timeBased();
        SubmitJobRequest currentStep = factory.createRequest(datasetId, executionId, previousExecutionId, parallelism);
        LOGGER.info("{} {}", currentStep.getEntryClass(), currentStep.getProgramArgs());
        arguments = currentStep.getProgramArgs().split(" ");
        GenericJob nextStep = GenericJobFactory.createJob(currentStep.getEntryClass());
        jobExecutionResult = nextStep.executeJob(arguments);
        LOGGER.info("result {} {}", currentStep.getEntryClass(), jobExecutionResult);
      }
      LOGGER.info("Completed workflow.");
    } catch (Exception e) {
      LOGGER.error("Workflow ended with an error!", e);
    }
  }
}
