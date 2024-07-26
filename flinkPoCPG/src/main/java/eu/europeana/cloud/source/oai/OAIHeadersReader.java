package eu.europeana.cloud.source.oai;

import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.METADATA_PREFIX;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.OAI_REPOSITORY_URL;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.SET_SPEC;

import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnectionProvider;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIHeadersReader implements SourceReader<OaiRecordHeader, OAISplit> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIHeadersReader.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;
  private final SourceReaderContext context;
  private final ParameterTool parameterTool;
  private boolean active;

  private CompletableFuture<Void> available = new CompletableFuture<>();
  private List<OAISplit> splits = new ArrayList<>();
  private boolean completed;
  private OaiHarvester harvester;
  private OaiHarvest oaiHarvest;
  BlockingDeque<OaiRecordHeader> deque = new LinkedBlockingDeque<>(100);
  private OAISplit harvestedSplit;
  private int count;
  private Map<Long, OAIIterationState> checkPointStates = new HashMap<>();

  private AtomicBoolean harvestingInBackground = new AtomicBoolean(false);
  private DbConnectionProvider dbConnectionProvider;
  private TaskInfoRepository taskInfoRepository;
  public OAIHeadersReader(SourceReaderContext context, ParameterTool parameterTool) {
    this.context = context;
    this.parameterTool = parameterTool;
    LOGGER.info("Created oai reader.");
  }

  @Override
  public void start() {
    LOGGER.info("Started oai reader.");
    dbConnectionProvider = new DbConnectionProvider(parameterTool);
    taskInfoRepository=new TaskInfoRepository(dbConnectionProvider);
    harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
    oaiHarvest = new OaiHarvest(
        parameterTool.getRequired(OAI_REPOSITORY_URL),
        parameterTool.getRequired(METADATA_PREFIX),
        parameterTool.getRequired(SET_SPEC));
  }

  @Override
  public InputStatus pollNext(ReaderOutput<OaiRecordHeader> output) throws Exception {
    if (completed) {
      LOGGER.info("Poll on completed OAI source.");
      return InputStatus.END_OF_INPUT;
    }
    LOGGER.info("Executed poll: active: {}", active);
    if (!active) {
      available = new CompletableFuture<>();
      context.sendSplitRequest();
      return InputStatus.NOTHING_AVAILABLE;
    }

    OaiRecordHeaderIterator headerIterator = harvester.harvestRecordHeaders(oaiHarvest);
    headerIterator.forEach(oaiHeader -> {
      output.collect(oaiHeader);
      return IterationResult.CONTINUE;
    });
    headerIterator.close();
    active = false;
    available = CompletableFuture.completedFuture(null);
    completed=true;

    LOGGER.info("Completed OAI source.");
    return InputStatus.END_OF_INPUT;
  }

  @Override
  public InputStatus pollNext(ReaderOutput<OaiRecordHeader> output) throws Exception {
    if (splits.isEmpty()) {
      available = new CompletableFuture<>();
      context.sendSplitRequest();
      LOGGER.debug("<{}> Executed poll splits empty, harvestedSplit: {}, returning: {}", System.identityHashCode(this),
          harvestedSplit, InputStatus.NOTHING_AVAILABLE);
      return InputStatus.NOTHING_AVAILABLE;
    } else {
      LOGGER.debug("<{}> Executed poll, assigned splits: {}, harvestedSplit: {}", System.identityHashCode(this), splits,
          harvestedSplit);
      if (harvestedSplit == null) {
        harvestedSplit = splits.get(0);
        harvestSourceInBackground();
      }
      boolean harvesting = harvestingInBackground.get();
      OaiRecordHeader header = deque.poll(10, TimeUnit.SECONDS);
      LOGGER.info("<{}> Polled from deque: {}", System.identityHashCode(this), header);
      if (header != null) {
        //        Thread.sleep(1000L);
        output.collect(header);
        count++;
        lastOaiIdentifier = header.getOaiIdentifier();
        return InputStatus.MORE_AVAILABLE;
      } else if (harvesting) {
        return InputStatus.MORE_AVAILABLE;
      } else {
        harvestedSplit = null;
        splits.remove(0);
        return InputStatus.END_OF_INPUT;
      }

    }
  }

  private void harvestSourceInBackground() {

    OAIIterationState restoredState = readStateOfPreviousTaskExecutionFromDB();
    if(restoredState!=null) {
      count = restoredState.getCompletedCount();
    }
    LOGGER.info("<{}> Restored state: {}", System.identityHashCode(this), restoredState);
    LOGGER.info("Submitting background harvesting...");
    harvestingInBackground.set(true);
    ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable,
        "Reading OAI source: " + System.identityHashCode(this)));
    executor.submit( () ->      harvestSource(restoredState));
    executor.shutdown();
  }

  private void harvestSource(OAIIterationState restoredState) {
    try {
      LOGGER.info("<{}> Starting background harvesting...", System.identityHashCode(this));
      AtomicBoolean restored = new AtomicBoolean(false);
      OaiHarvester harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
      OaiRecordHeaderIterator headerIterator = harvester.harvestRecordHeaders(taskParams.getOaiHarvest());

      headerIterator.forEach(oaiHeader -> {
        if (restoredState != null && !restored.get()) {
          if (oaiHeader.getOaiIdentifier().equals(restoredState.getLastIdentifier())) {
            restored.set(true);
          }
          return IterationResult.CONTINUE;
        }

        try {
          deque.put(oaiHeader);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return IterationResult.CONTINUE;
      });
      headerIterator.close();
    } catch (Throwable e) {
      LOGGER.error("<{}>Could not complete harvesting.", System.identityHashCode(this), e);
    } finally {
      harvestingInBackground.set(false);
    }
  }

  @Override
  public List<OAISplit> snapshotState(long checkpointId) {
    if(lastOaiIdentifier!=null) {
      checkPointStates.put(checkpointId, OAIIterationState.builder()
                                                          .datasetId(taskParams.getDatasetId())
                                                          .executionId(taskParams.getExecutionId())
                                                          .lastIdentifier(lastOaiIdentifier)
                                                          .completedCount(count)
                                                          .build());
    }
    LOGGER.info("Snapshotted state: {}, lastEmittedIdentifier: {} splits: {}", checkpointId, lastOaiIdentifier, splits);
    return splits;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    LOGGER.info("notifyCheckpointComplete: {}", checkpointId);
    OAIIterationState state = checkPointStates.remove(checkpointId);
    if (state != null) {
      taskInfoRepository.update(state.getExecutionId());
      );
    }
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return available;
  }

  @Override
  public void addSplits(List<OAISplit> splits) {
    LOGGER.info("Adding splits: {}", splits);
    this.splits.addAll(splits);
    available.complete(null);
    LOGGER.info("Added split");
  }


  @Override
  public void notifyNoMoreSplits() {
    LOGGER.info("Notified: no more splits");
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("<{}>Closing reader...", System.identityHashCode(this));
    dbConnectionProvider.close();
    LOGGER.info("Closed reader!");
  }

  private OAIIterationState readStateOfPreviousTaskExecutionFromDB() {
    ResultSet result = session.execute(
        readStateStatement.bind(taskParams.getDatasetId(), taskParams.getExecutionId().toString()));
    Row row = result.one();
    if (row != null) {
      return OAIIterationState.builder()
                              .datasetId(row.getString(0))
                              .executionId(UUID.fromString(row.getString(1)))
                              .completedCount(row.getInt(2))
                              .lastIdentifier(row.getString(3))
                              .build();
    } else {
      return null;
    }
  }
}
