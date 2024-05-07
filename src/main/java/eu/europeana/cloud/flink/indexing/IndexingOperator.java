package eu.europeana.cloud.flink.indexing;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.tiers.model.MediaTier;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingOperator extends FollowingJobMainOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingOperator.class);

  private final IndexingTaskParams taskParams;
  private transient Indexer indexer;
  private transient IndexingProperties executionIndexingProperties;

  public IndexingOperator(IndexingTaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public RecordTuple map(RecordTuple tuple) {
    try {
      boolean recordNotSuitableForPublication = !indexRecord(tuple);
      if (recordNotSuitableForPublication) {
        removeIndexedRecord(tuple);
        throw new RuntimeException("Record deleted from database " + taskParams.getDatabase()
            + ", cause it was in media tier 0! Id: " + tuple.getRecordId());
      }
      return tuple;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return RecordTuple.builder()
                        .recordId(tuple.getRecordId())
                        .fileContent(tuple.getFileContent())
                        .errorMessage(e.getMessage())
                        .build();
    }
  }

  @Override
  public void open(Configuration parameters) {
    try {
      executionIndexingProperties = new IndexingProperties(taskParams.getRecordDate(), taskParams.isPreserveTimestamps(),
          taskParams.getDatasetIdsForRedirection(), taskParams.isPerformRedirects(), true);
      IndexerFactory indexerFactory = new IndexerFactory(prepareIndexingSettings());
      indexer = indexerFactory.getIndexer();
      LOGGER.info("Created indexing operator.");
    } catch (Exception e) {
      LOGGER.warn("Indexing service not available {}", e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception {
    indexer.close();
    LOGGER.info("Closed indexing operator.");
  }

  private boolean indexRecord(RecordTuple tuple) throws IndexingException {
    AtomicBoolean suitableForPublication = new AtomicBoolean();
    final var document = new String(tuple.getFileContent(), StandardCharsets.UTF_8);
    indexer.index(document, executionIndexingProperties, tier -> {
      suitableForPublication.set(
          (taskParams.getDatabase() == TargetIndexingDatabase.PREVIEW) || (tier.getMediaTier() != MediaTier.T0));
      return suitableForPublication.get();
    });
    return suitableForPublication.get();
  }

  private void removeIndexedRecord(RecordTuple tuple)
      throws IndexingException {
    final String europeanaId = tuple.getRecordId();
    LOGGER.debug("Removing indexed record europeanaId: {}, database: {}", europeanaId, taskParams.getDatabase());
    indexer.remove(europeanaId);
  }

  private IndexingSettings prepareIndexingSettings() throws IndexingException, URISyntaxException {
    IndexingSettingsGenerator settingsGenerator = new IndexingSettingsGenerator(taskParams.getIndexingProperties());
    return taskParams.getDatabase() == TargetIndexingDatabase.PREVIEW
        ? settingsGenerator.generateForPreview()
        : settingsGenerator.generateForPublish();
  }
}
