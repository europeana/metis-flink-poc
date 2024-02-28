package eu.europeana.cloud.flink.indexing;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingJob extends AbstractFollowingJob<IndexingTaskParams> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingJob.class);

  public IndexingJob(Properties properties, IndexingTaskParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected IndexingOperator createMainOperator(Properties properties, IndexingTaskParams taskParams) {
    return new IndexingOperator(taskParams);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired("datasetId");

    IndexingTaskParams taskParams = IndexingTaskParams
        .builder()
        .datasetId(metisDatasetId)
        .metisDatasetId(metisDatasetId)
        .previousStepId(UUID.fromString(tool.getRequired("previousStepId")))
        .database(TargetIndexingDatabase.valueOf(tool.getRequired("targetIndexingDatabase")))
        .recordDate(Optional.ofNullable(tool.get("recordDate")).map(DateHelper::parseISODate).orElse(new Date()))
        .preserveTimestamps(tool.getBoolean("preserveTimestamps", false))
        .performRedirects(tool.getBoolean("performRedirects", false))
        .datasetIdsForRedirection(Optional.ofNullable(tool.get("datasetIdsToRedirectFrom"))
                                          .map(param -> Arrays.stream(param.split(","))
                                                              .map(String::trim)
                                                              .toList())
                                          .orElse(Collections.emptyList()))
        .indexingProperties(readProperties(tool.getRequired("indexingPropertiesFilePath")))
        .build();
    IndexingJob job = new IndexingJob(readProperties(tool.getRequired("configurationFilePath")), taskParams);
    job.execute();
  }

}
