package eu.europeana.cloud.source;

import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.retryable.RetryableMethodExecutor;
import eu.europeana.cloud.tool.DbConnectionProvider;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.jetbrains.annotations.NotNull;
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
  private final long taskId;

  ExecutionRecordRepository executionRecordRepository;
  TaskInfoRepository taskInfoRepo;
  private DbConnectionProvider dbConnectionProvider;

  private long recordsToBeProcessed;
  private long allPartitionCount;
  private long startedPartitionCount;
  private long finishedRecordCount;
  private final NavigableMap<Long, Long> checkpointIdToFinishedRecordCountMap = new TreeMap<>();
  private long commitCount;
  private final List<DataPartition> returnedPartitions;
  private final Map<DataPartition, SplitProgressInfo> executingPartitions = new LinkedHashMap<>();

  public DbEnumerator(SplitEnumeratorContext<DataPartition> context, DbEnumeratorState state,
      ParameterTool parameterTool) {
    this.context = context;
    this.parameterTool = parameterTool;
    this.taskId = parameterTool.getLong(JobParamName.TASK_ID);
    this.chunkSize = parameterTool.getInt(JobParamName.CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
    if (state != null) {
      recordsToBeProcessed = state.getRecordsToBeProcessed();
      allPartitionCount = state.getAllPartitionCount();
      startedPartitionCount = state.getStartedPartitionCount();
      finishedRecordCount = state.getFinishedRecordCount();
      commitCount = state.getCommitCount();
      returnedPartitions = state.getIncompletePartitions();
      LOGGER.info(
          "Created DbEnumerator with finished: {} records, and: {} of: {} all partitions, {} started, from which: {} are incomplete: {}",
          finishedRecordCount, getFinishedPartitionCount(), allPartitionCount, startedPartitionCount, returnedPartitions.size(),
          returnedPartitions);
    } else {
      recordsToBeProcessed = NOT_EVALUATED;
      allPartitionCount = NOT_EVALUATED;
      startedPartitionCount = 0;
      finishedRecordCount = 0;
      commitCount = 0;
      returnedPartitions = new ArrayList<>();
      LOGGER.info("Created DbEnumerator with no fetched partitions");
    }

  }

  @Override
  public void start() {
    LOGGER.info("Starting DbEnumerator");
    dbConnectionProvider = new DbConnectionProvider(parameterTool);
    executionRecordRepository = RetryableMethodExecutor.createRetryProxy(new ExecutionRecordRepository(dbConnectionProvider));
    taskInfoRepo = RetryableMethodExecutor.createRetryProxy(new TaskInfoRepository(dbConnectionProvider));
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
//    if (Math.random() < 0.333) {
//      throw new RuntimeException("Artificial exception: DBEnumerator failed on assigning new partition!");
//    }

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
    executingPartitions.put(splitToBeServed, new SplitProgressInfo());
    context.assignSplit(splitToBeServed, subtaskId);
    LOGGER.info("Assigned split: {} for subtaskId: {}, host: {}. Executing: {} of: {} started: splits, finished: {}",
        splitToBeServed, subtaskId, requesterHostname, executingPartitions.size(), startedPartitionCount,
        getFinishedPartitionCount());
  }

  @Override
  public void addSplitsBack(List<DataPartition> splits, int subtaskId) {
    for (DataPartition split : splits) {
      addSplitBack(split, subtaskId);
    }

  }

  private void addSplitBack(DataPartition split, int subtaskId) {
    SplitProgressInfo info = removeSplitFromExecutingMap(split);
    DataPartition updatedSplit = createSplitWithoutCompletedRecords(split, info);
    if (split.limit() > 0) {
      returnedPartitions.add(updatedSplit);
      LOGGER.info(
          "Added split: {} from subtask: {} back. Updated split: {}. Currently executing: {} splits, all returned splits: {}",
          split, subtaskId, updatedSplit, executingPartitions.size(), returnedPartitions.size());
    } else {
      LOGGER.warn("Added split: {} from subtask: {} back, but the subtask is already completed! Info: {}."
              + "Currently executing: {} splits, all returned splits: {}",
          subtaskId, split, info, executingPartitions.size(), returnedPartitions.size());

    }
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof ProgressSnapshotEvent event) {
      handleProgressSnapshotEvent(event);
    } else if (sourceEvent instanceof SplitCompletedEvent event) {
      handleSplitCompletedEvent(event);
    }
  }

  private void handleProgressSnapshotEvent(ProgressSnapshotEvent event) {
    SplitProgressInfo progressInfo = getSplitFromExecutingMap(event);
    finishedRecordCount += progressInfo.update(event);
    LOGGER.info("Received progress information: {}", event);
  }

  private void handleSplitCompletedEvent(SplitCompletedEvent event) {
    DataPartition split = event.getSplit();
    SplitProgressInfo info = removeSplitFromExecutingMap(split);
    finishedRecordCount += info.update(event);
    LOGGER.info("Split completed: {}. Now executing {} splits. Finished {} of: {} all splits.",
        event, executingPartitions.size(), getFinishedPartitionCount(), allPartitionCount);
  }

  @Override
  public void addReader(int subtaskId) {
    LOGGER.info("New reader added for, the subtaskId: {}", subtaskId);
  }

  @Override
  public DbEnumeratorState snapshotState(long checkpointId) {
    DbEnumeratorState state = DbEnumeratorState.builder()
                                               .recordsToBeProcessed(recordsToBeProcessed)
                                               .allPartitionCount(allPartitionCount)
                                               .startedPartitionCount(startedPartitionCount)
                                               .finishedRecordCount(finishedRecordCount)
                                               .commitCount(commitCount)
                                               .incompletePartitions(getIncompletePartitionsSnapshot())
                                               .build();
    checkpointIdToFinishedRecordCountMap.put(checkpointId, finishedRecordCount);
    LOGGER.info("Creating snapshot of state for the checkpoint: {}, state: {}", checkpointId, state);
    return state;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    LOGGER.info("Checkpoint: {} completed. Updating progress... Map:{}", checkpointId,checkpointIdToFinishedRecordCountMap);
    SortedMap<Long, Long> approvedProgresses = checkpointIdToFinishedRecordCountMap.headMap(checkpointId, true);
    Map.Entry<Long, Long> lastApprovedProgress = approvedProgresses.lastEntry();
    if (lastApprovedProgress != null) {
      //TODO Commit count is not strictly evaluated cause in case of restart of job in case of exception
      //This value is lost and set to the last snapshot value. So we need to increase it earlier
      // during snapshot start and store here but we also need not increase it again if snapshot
      // is aborted, so it is a bit difficult.
      //We could consider small optimisation to not save progress if there is no change in it.
      //Then for example we could save modification date to db, for debug purpose.
      TaskInfo taskInfo = new TaskInfo(taskId, ++commitCount, lastApprovedProgress.getValue());

      //TODO The repository uses retries in case of failure, but because updating progress is not a key feature,
      // without it the task should finish its work properly. Beside that we could omit some updates of progress
      // as long as we store last progress, when the task is whole complete.
      // So we could consider more sophisticated failover mechanism with lesser impact on the execution.
      taskInfoRepo.update(taskInfo);

      approvedProgresses.clear();
      LOGGER.info("Updated task progress in DB: {}", taskInfo);
    } else {
      LOGGER.info("There is not approved progress to update in DB for checkpoint id: {}. Progress map: {}",
          checkpointId, checkpointIdToFinishedRecordCountMap);
    }

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

  private List<DataPartition> getIncompletePartitionsSnapshot() {
    List<DataPartition> incompletePartitions = new ArrayList<>();
    for (Entry<DataPartition, SplitProgressInfo> entry : executingPartitions.entrySet()) {
      DataPartition split = createSplitWithoutCompletedRecords(entry.getKey(), entry.getValue());
      if (split.limit() > 0) {
        incompletePartitions.add(split);
      }
    }
    incompletePartitions.addAll(returnedPartitions);
    return incompletePartitions;
  }

  private void validateTaskExists() {
    if(taskInfoRepo.findById(taskId).isEmpty()){
      LOGGER.error("Task not found in the database. It should never happen.");
      System.exit(1);
    }
  }

  private SplitProgressInfo getSplitFromExecutingMap(ProgressSnapshotEvent event) {
    DataPartition split = event.getSplit();
    SplitProgressInfo progressInfo = executingPartitions.get(split);
    if (progressInfo == null) {
      throw new SourceConsistencyException("Could not find the split in the executing map, for received progress event: "
          + event + ". Presence of split in the returned list: " + returnedPartitions.contains(split));
    }
    return progressInfo;
  }

  private SplitProgressInfo removeSplitFromExecutingMap(DataPartition split) {
    SplitProgressInfo info = executingPartitions.remove(split);
    if (info == null) {
      //TODO Check if it could happen anyway. Maybe we coudl ignore it, cause split is already completed
      throw new SourceConsistencyException("Could not find split: " + split + " in the executed splits!");
    }
    return info;
  }

  private long getFinishedPartitionCount() {
    return startedPartitionCount - returnedPartitions.size() - executingPartitions.size();
  }

  /**
   * Method trim partition to not contain already completed records. it is needed in case when we need
   * to retry given partition for example after job restarting. The result of this method could be split
   * with limit 0 in rare cases.
   * @param split - original split
   * @param info - info about progress
   * @return new trimmed split.
   */
  private static @NotNull DataPartition createSplitWithoutCompletedRecords(DataPartition split, SplitProgressInfo info) {
    return new DataPartition(split.offset() + info.getEmittedRecordCount(), split.limit() - info.getEmittedRecordCount());
  }

}
