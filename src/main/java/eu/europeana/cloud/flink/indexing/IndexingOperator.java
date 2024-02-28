package eu.europeana.cloud.flink.indexing;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.tiers.model.MediaTier;
import eu.europeana.metis.transformation.service.TransformationException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingOperator extends FollowingJobMainOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingOperator.class);

  private final IndexingTaskParams taskParams;
  private transient IndexWrapper indexWrapper;
  private transient IndexingProperties executionIndexingProperties;
  public IndexingOperator(IndexingTaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public RecordTuple map(RecordTuple tuple) throws Exception {
    boolean recordNotSuitableForPublication = !indexRecord(tuple);
    if (recordNotSuitableForPublication) {
      removeIndexedRecord(tuple);
      throw new RuntimeException("Record deleted from database " + taskParams.getDatabase()
          + ", cause it was in media tier 0! Id: " + tuple.getRecordId());
    }
    return tuple;
  }

  private boolean indexRecord(RecordTuple tuple) throws IndexingException {
    AtomicBoolean suitableForPublication = new AtomicBoolean();
    final var document = new String(tuple.getFileContent(), StandardCharsets.UTF_8);
    indexWrapper.getIndexer(taskParams.getDatabase()).index(document, executionIndexingProperties, tier -> {
      suitableForPublication.set(
          (taskParams.getDatabase() == TargetIndexingDatabase.PREVIEW) || (tier.getMediaTier() != MediaTier.T0));
      return suitableForPublication.get();
    });
    return suitableForPublication.get();
  }

  private void removeIndexedRecord(RecordTuple tuple)
      throws IndexingException {
    String europeanaId = tuple.getRecordId();
    LOGGER.debug("Removing indexed record europeanaId: {}, database: {}", europeanaId, taskParams.getDatabase());
    indexWrapper.getIndexer(taskParams.getDatabase()).remove(europeanaId);
  }

  public void open(Configuration parameters) throws TransformationException {
    executionIndexingProperties = new IndexingProperties(taskParams.getRecordDate(), taskParams.isPreserveTimestamps(),
        taskParams.getDatasetIdsForRedirection(), taskParams.isPerformRedirects(), true);
    indexWrapper = IndexWrapper.getInstance(taskParams.getIndexingProperties());
    LOGGER.info("Created indexing operator.");
  }

  @Override
  public void close() throws Exception {
   indexWrapper.close();
  }
}
