package eu.europeana.cloud.flink.indexing;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_IDS_TO_REDIRECT_FROM;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.EXECUTION_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.INDEXING_PROPERTIES_FILE_PATH;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PARALLELISM;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PERFORM_REDIRECTS;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PRESERVE_TIMESTAMPS;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PREVIOUS_STEP_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.RECORD_DATE;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.TARGET_INDEXING_DATABASE;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;
import static eu.europeana.cloud.flink.common.utils.JobUtils.useNewIfNull;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.JobParameters;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class IndexingJob extends AbstractFollowingJob<IndexingTaskParams> {

  public static void main(String[] args) throws Exception {
    IndexingJob indexing = new IndexingJob();
    indexing.executeJob(args);
  }

  protected String mainOperatorName() {
    return "Index"; //TODO into preview.
  }

  @Override
  protected IndexingOperator createMainOperator(Properties properties, IndexingTaskParams taskParams) {
    return new IndexingOperator(taskParams);
  }

  @Override
  protected JobParameters<IndexingTaskParams> prepareParameters(String[] args) throws IOException {
    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired(DATASET_ID);

    IndexingTaskParams taskParams = IndexingTaskParams
        .builder()
        .datasetId(metisDatasetId)
        .metisDatasetId(metisDatasetId)
        .executionId(useNewIfNull(tool.get(EXECUTION_ID)))
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .database(TargetIndexingDatabase.valueOf(tool.getRequired(TARGET_INDEXING_DATABASE)))
        .recordDate(Optional.ofNullable(tool.get(RECORD_DATE)).map(DateHelper::parseISODate).orElse(new Date()))
        .preserveTimestamps(tool.getBoolean(PRESERVE_TIMESTAMPS, false))
        .performRedirects(tool.getBoolean(PERFORM_REDIRECTS, false))
        .datasetIdsForRedirection(Optional.ofNullable(tool.get(DATASET_IDS_TO_REDIRECT_FROM))
                                          .map(param -> Arrays.stream(param.split(","))
                                                              .map(String::trim)
                                                              .toList())
                                          .orElse(Collections.emptyList()))
        .indexingProperties(readProperties(tool.getRequired(INDEXING_PROPERTIES_FILE_PATH)))
        .parallelism(tool.getInt(PARALLELISM, 1))
        .build();
    return new JobParameters<>(tool, taskParams);
  }
}
