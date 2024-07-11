package eu.europeana.cloud.flink.workflow;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.CONFIGURATION_FILE_PATH;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

import eu.europeana.cloud.flink.client.JobExecutor;
import eu.europeana.cloud.flink.client.entities.JobDetails;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.regex.Pattern;

import eu.europeana.cloud.repository.ExecutionRecordExceptionLogRepository;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.tool.DbConnectionProvider;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressReport {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void main(String[] args) throws IOException {
    final String jobId = "171de25fe70bf53dff625d545aed6eac";
    final String datasetId = "1";
    final String providedSourceExecutionId = "5b4afb60-27d6-11ef-b3e7-3fe03a920947";
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    Properties properties = readProperties(parameterTool.getRequired(CONFIGURATION_FILE_PATH));
    final JobExecutor jobExecutor = new JobExecutor(properties);
    final JobDetails jobDetails = jobExecutor.getProgress(jobId);

    final String targetExecutionId = extractExecutionId(jobDetails.getName());
    final String sourceExecutionId;
    if (Pattern.compile(Pattern.quote("OAI"), Pattern.CASE_INSENSITIVE).matcher(jobDetails.getName()).find()){
      //For OAI we don't know the source size so we the source is updated at the same time as the target instead of showing 0.
      sourceExecutionId = targetExecutionId;
    } else {
      sourceExecutionId = providedSourceExecutionId;
    }

    await().forever().until(() -> {
      final JobDetails jobDetailsInternal = jobExecutor.getProgress(jobId);
      printProgress(datasetId, sourceExecutionId, targetExecutionId, parameterTool);
      return jobDetailsInternal.getState().equals("FINISHED");
    });
  }

  private static void printProgress(String datasetId, String sourceExecutionId,
      String targetExecutionId, ParameterTool parameterTool) {

    try {
      ExecutionRecordRepository executionRecordRepository = new ExecutionRecordRepository(new DbConnectionProvider(parameterTool));
      ExecutionRecordExceptionLogRepository executionRecordExceptionLogRepository = new ExecutionRecordExceptionLogRepository(new DbConnectionProvider(parameterTool));
      final long sourceTotal = executionRecordRepository.countByDatasetIdAndExecutionId(datasetId, sourceExecutionId);
      final long processedSuccess = executionRecordRepository.countByDatasetIdAndExecutionId(datasetId, targetExecutionId);
      final long processedException = executionRecordExceptionLogRepository.countByDatasetIdAndExecutionId(datasetId, sourceExecutionId);
      final long processed = processedSuccess + processedException;
      LOGGER.info(
              format("Task progress - Processed/SourceTotal: %s/%s, Exceptions: %s", processed, sourceTotal, processedException));

    } catch (IOException e) {
      LOGGER.error("Unable to read progress", e);
    }
  }

  private static String extractExecutionId(String jobName) {
    String startToken = "execution: ";
    int startIndex = jobName.indexOf(startToken);

    startIndex += startToken.length();
    int endIndex = jobName.indexOf(")", startIndex);
    return jobName.substring(startIndex, endIndex).trim();
  }
}
