package eu.europeana.cloud.flink.workflow;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.CONFIGURATION_FILE_PATH;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import eu.europeana.cloud.flink.client.JobExecutor;
import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.client.entities.JobDetails;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.regex.Pattern;
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
    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);

    await().forever().until(() -> {
      final JobDetails jobDetailsInternal = jobExecutor.getProgress(jobId);
      printProgress(cassandraClusterBuilder, datasetId, sourceExecutionId, targetExecutionId);
      return jobDetailsInternal.getState().equals("FINISHED");
    });
  }

  private static void printProgress(CassandraClusterBuilder cassandraClusterBuilder, String datasetId, String sourceExecutionId,
      String targetExecutionId) {
    final ResultSet resultSetSource;
    final ResultSet resultSetSuccess;
    final ResultSet resultSetException;
    try (Session session = cassandraClusterBuilder.getCluster().connect()) {
      resultSetSource = session.execute(
          format("SELECT COUNT(*) FROM flink_poc.execution_record WHERE dataset_id='%s' AND execution_id='%s';", datasetId,
              sourceExecutionId));
      resultSetSuccess = session.execute(
          format("SELECT COUNT(*) FROM flink_poc.execution_record WHERE dataset_id='%s' AND execution_id='%s';", datasetId,
              targetExecutionId));
      resultSetException = session.execute(
          format("SELECT COUNT(*) FROM flink_poc.execution_record_exception_log WHERE dataset_id='%s' AND execution_id='%s';",
              datasetId, targetExecutionId));
      session.getCluster().close();
    }

    final long sourceTotal = resultSetSource.one().getLong("count");
    final long processedSuccess = resultSetSuccess.one().getLong("count");
    final long processedException = resultSetException.one().getLong("count");
    final long processed = processedSuccess + processedException;
    LOGGER.info(
        format("Task progress - Processed/SourceTotal: %s/%s, Exceptions: %s", processed, sourceTotal, processedException));
  }

  private static String extractExecutionId(String jobName) {
    String startToken = "execution: ";
    int startIndex = jobName.indexOf(startToken);

    startIndex += startToken.length();
    int endIndex = jobName.indexOf(")", startIndex);
    return jobName.substring(startIndex, endIndex).trim();
  }
}
