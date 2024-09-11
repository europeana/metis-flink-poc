package eu.europeana.cloud.source;

import eu.europeana.cloud.exception.TaskInfoNotFoundException;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnectionProvider;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DbEnumerator implements SplitEnumerator<DataPartition, DbEnumeratorState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DbEnumerator.class);
  private static final int DEFAULT_CHUNK_SIZE = 1000;
  public static final int NOT_EVALUATED = -1;

  private final SplitEnumeratorContext<DataPartition> context;
  private final ParameterTool parameterTool;
  private final int chunkSize;

  ExecutionRecordRepository executionRecordRepository;
  TaskInfoRepository taskInfoRepo;
  private DbConnectionProvider dbConnectionProvider;

  private long recordsToBeProcessed;
  private long allPartitionCount;
  private long startedPartitionCount;
  private final List<DataPartition> returnedPartitions;
  private final List<DataPartition> executingPartitions = new ArrayList<>();

  public DbEnumerator(SplitEnumeratorContext<DataPartition> context, DbEnumeratorState state,
      ParameterTool parameterTool) {
    this.context = context;
    this.parameterTool = parameterTool;
    this.chunkSize = parameterTool.getInt(JobParamName.CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
    if (state != null) {
      recordsToBeProcessed = state.getRecordsToBeProcessed();
      allPartitionCount = state.getAllPartitionCount();
      startedPartitionCount = state.getStartedPartitionCount();
      returnedPartitions = state.getIncompletePartitions();
      LOGGER.info("Created DbEnumerator with finished: {} of: {} all partitions, {} started, from which: {} are incomplete: {}",
          getFinishedPartitionCount(), allPartitionCount, startedPartitionCount, returnedPartitions.size(), returnedPartitions);
    } else {
      recordsToBeProcessed = NOT_EVALUATED;
      allPartitionCount = NOT_EVALUATED;
      startedPartitionCount = 0;
      returnedPartitions = new ArrayList<>();
      LOGGER.info("Created DbEnumerator with no fetched partitions");
    }

  }

  @Override
  public void start() {
    LOGGER.info("Starting DbEnumerator");
    dbConnectionProvider = new DbConnectionProvider(parameterTool);
    executionRecordRepository = new ExecutionRecordRepository(dbConnectionProvider);
    taskInfoRepo = new TaskInfoRepository(dbConnectionProvider);
    validateTaskExists();
    if (allPartitionCount == NOT_EVALUATED) {
      evaluateSplitCount();
    } else {
      LOGGER.info("Splits are already initialized. Finished: {} of {} all partitions.",
          getFinishedPartitionCount(), allPartitionCount);
    }
  }

  @Override
  public void handleSplitRequest(int subtaskId, String requesterHostname) {
    DataPartition splitToBeServed;
    if (!returnedPartitions.isEmpty()) {
      splitToBeServed = returnedPartitions.removeFirst();
    } else if (startedPartitionCount < allPartitionCount) {
      splitToBeServed = new DataPartition(startedPartitionCount * chunkSize, chunkSize);
      startedPartitionCount++;
    } else {
      LOGGER.info("No more remaining splits, {} splits executing!", executingPartitions.size());
      context.signalNoMoreSplits(subtaskId);
      return;
    }
    executingPartitions.add(splitToBeServed);
    context.assignSplit(splitToBeServed, subtaskId);
    LOGGER.info("Assigned split: {} for subtaskId: {}, host: {}. Executing: {} of: {} started: splits, finished: {}",
        splitToBeServed, subtaskId, requesterHostname, executingPartitions.size(), startedPartitionCount,
        getFinishedPartitionCount());
  }

  @Override
  public void addSplitsBack(List<DataPartition> splits, int subtaskId) {
    returnedPartitions.addAll(splits);
    executingPartitions.removeAll(splits);
    LOGGER.info("Added splits the subtask: {} back: {}. Executing: {} splits, all returned splits: {}",
        subtaskId, splits, executingPartitions.size(), returnedPartitions.size());
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof SplitCompletedEvent event) {
      boolean removed = executingPartitions.remove(event.getCurrentSplit());
      LOGGER.info("Removed ({}) completed split: {}. Now executing {} splits. Finished {} of: {} all splits.",
          removed, event.getCurrentSplit(), executingPartitions.size(), getFinishedPartitionCount(), allPartitionCount);
    }

  }

  @Override
  public void addReader(int subtaskId) {
    LOGGER.info("New reader added for, the subtaskId: {}", subtaskId);
  }

  @Override
  public DbEnumeratorState snapshotState(long checkpointId) {
    ArrayList<DataPartition> incompletePartitions = new ArrayList<>();
    incompletePartitions.addAll(executingPartitions);
    incompletePartitions.addAll(returnedPartitions);
    DbEnumeratorState state = DbEnumeratorState.builder()
                                               .recordsToBeProcessed(recordsToBeProcessed)
                                               .allPartitionCount(allPartitionCount)
                                               .startedPartitionCount(startedPartitionCount)
                                               .incompletePartitions(incompletePartitions).build();
    LOGGER.info("Creating snapshot of state for the checkpoint: {}, state: {}", checkpointId, state);
    return state;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    LOGGER.info("Checkpoint completed: {}", checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) {
    LOGGER.info("Checkpoint aborted: {}!", checkpointId);
  }

  @Override
  public void close() throws IOException {
    try {
      dbConnectionProvider.close();
    } catch (Exception e) {
      throw new IOException("Could not close dbProvider!", e);
    }
  }

  private void evaluateSplitCount() {
    LOGGER.info("Preparing split count...");
    try {
      //TODO size of the split should be adjusted to parallelization level to work optimal
      //Is good to do the adjustment in some place.
      recordsToBeProcessed = executionRecordRepository.countByDatasetIdAndExecutionId(
          parameterTool.getRequired(JobParamName.DATASET_ID),
          parameterTool.getRequired(JobParamName.EXECUTION_ID));
      allPartitionCount = (recordsToBeProcessed + chunkSize - 1) / chunkSize; //dividing with rounding up
      LOGGER.info("Finished, there is: {} records to be processed divided into: {} splits.",
          recordsToBeProcessed, allPartitionCount);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateTaskExists() {
    try {
      taskInfoRepo.get(parameterTool.getLong(JobParamName.TASK_ID));
    } catch (TaskInfoNotFoundException e) {
      LOGGER.error("Task not found in the database. It should never happen", e);
      System.exit(1);
    }
  }

  private long getFinishedPartitionCount() {
    return startedPartitionCount - returnedPartitions.size() - executingPartitions.size();
  }

}
