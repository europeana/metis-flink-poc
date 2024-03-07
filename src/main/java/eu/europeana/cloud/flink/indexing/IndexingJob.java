package eu.europeana.cloud.flink.indexing;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.*;
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
import org.apache.flink.api.java.utils.ParameterTool;

public class IndexingJob extends AbstractFollowingJob<IndexingTaskParams> {

  public IndexingJob(Properties properties, IndexingTaskParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  protected String mainOperatorName() {
    return "Index"; //TODO into preview.
  }

  @Override
  protected IndexingOperator createMainOperator(Properties properties, IndexingTaskParams taskParams) {
    return new IndexingOperator(taskParams);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired(DATASET_ID);

    IndexingTaskParams taskParams = IndexingTaskParams
        .builder()
        .datasetId(metisDatasetId)
        .metisDatasetId(metisDatasetId)
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
        .build();
    IndexingJob job = new IndexingJob(readProperties(tool.getRequired(CONFIGURATION_FILE_PATH)), taskParams);
    job.execute();
  }

}
