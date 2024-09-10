package eu.europeana.cloud.source;

import eu.europeana.cloud.flink.client.constants.postgres.JobParam;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnectionProvider;
import java.io.IOException;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class DbReaderWithProgressHandling implements SourceReader<ExecutionRecord, DataPartition> {

    private static final long INITIAL_CHECKPOINT_ID = -1;
    private final SourceReaderContext context;
    private final ParameterTool parameterTool;
    private ExecutionRecordRepository executionRecordRepository;
    private TaskInfoRepository taskInfoRepository;
    private CompletableFuture<Void> readerAvailable = new CompletableFuture<>();
    private final int maxRecordPending;
    private int currentRecordPendingCount;
    private int allCommitedRecordCount;
    private int currentSplitCommitedRecordCount;
    private final TreeMap<Long, Integer> recordPendingCountPerCheckpoint = new TreeMap<>();
    private boolean splitFetched = false;
    private boolean noMoreSplits = false;
    private long currentCheckpointId = INITIAL_CHECKPOINT_ID;
    private final long taskId;

    private List<ExecutionRecord> polledRecords = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DbReaderWithProgressHandling.class);

    private List<DataPartition> currentSplits = new ArrayList<>();
    private DbConnectionProvider dbConnectionProvider;
    private DataPartition currentSplit;

    public DbReaderWithProgressHandling(
            SourceReaderContext context,
            ParameterTool parameterTool) {
        this.context = context;
        this.parameterTool = parameterTool;
        taskId = parameterTool.getLong(JobParamName.TASK_ID);
        maxRecordPending = parameterTool.
                getInt(
                        JobParamName.MAX_RECORD_PENDING,
                        JobParam.DEFAULT_READER_MAX_RECORD_PENDING_COUNT);
    }

    @Override
    public void start() {
        LOGGER.info("Starting source reader");
        dbConnectionProvider = new DbConnectionProvider(parameterTool);
        executionRecordRepository = new ExecutionRecordRepository(dbConnectionProvider);
        taskInfoRepository = new TaskInfoRepository(dbConnectionProvider);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<ExecutionRecord> output) throws Exception {
        LOGGER.debug("Pooling next record");
        if (noMoreSplits) {
            LOGGER.debug("There are no more splits");
            return InputStatus.END_OF_INPUT;
        }
        if (!splitFetched) {
            LOGGER.debug("Fetching splits");
            context.sendSplitRequest();
            splitFetched = true;
        }

        if (!currentSplits.isEmpty()) {
            fetchRecordsIfNeeded();

            if (!polledRecords.isEmpty()) {
                ExecutionRecord executionRecord = polledRecords.removeFirst();
                emitRecord(output, executionRecord);
                if (isPendingLimitReached()) {
                    LOGGER.debug("Blocking reader due to hitting pending records limit");
                    blockReader();
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }else {
                LOGGER.debug("Removing split: {} due to exhaustion of polled record set"
                        + ", after commit: {} records of: {} all commited, ",
                   currentSplit , currentSplitCommitedRecordCount, allCommitedRecordCount);
                currentSplits.removeFirst();
                splitFetched = false;
                polledRecords = null;
                context.sendSourceEventToCoordinator(
                    new SplitCompletedEvent(currentSplit)
                );
                return InputStatus.MORE_AVAILABLE;
            }
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void emitRecord(ReaderOutput<ExecutionRecord> output, ExecutionRecord executionRecord) {
        currentRecordPendingCount++;
        int currentlyPendingForThisCheckpoint = 1;
        if (recordPendingCountPerCheckpoint.containsKey(currentCheckpointId)) {
            currentlyPendingForThisCheckpoint = recordPendingCountPerCheckpoint.get(currentCheckpointId);
            recordPendingCountPerCheckpoint.put(currentCheckpointId, ++currentlyPendingForThisCheckpoint);
        } else {
            recordPendingCountPerCheckpoint.put(currentCheckpointId, currentlyPendingForThisCheckpoint);
        }
        LOGGER.debug("Emitting record {} - currently pending {} records", executionRecord.getExecutionRecordKey().getRecordId(), currentRecordPendingCount);
        LOGGER.debug("There are {} records pending for checkpoint {}", currentlyPendingForThisCheckpoint, currentCheckpointId);
        output.collect(executionRecord);
    }

    private void fetchRecordsIfNeeded() throws IOException {
        currentSplit = currentSplits.getFirst();
        if (polledRecords == null) {
            LOGGER.debug("Fetching records from database");
            polledRecords = executionRecordRepository.getByDatasetIdAndExecutionIdAndOffsetAndLimit(
                    parameterTool.getRequired(JobParamName.DATASET_ID),
                    parameterTool.getRequired(JobParamName.EXECUTION_ID),
                    currentSplit.offset(), currentSplit.limit());

            currentSplitCommitedRecordCount = 0;
        } else {
            LOGGER.debug("Already fetched records exist");
        }
    }

    private boolean isPendingLimitReached() {
        if(currentRecordPendingCount >= maxRecordPending){
            LOGGER.debug("Pending limit: {} reached: {}", maxRecordPending, currentRecordPendingCount);
            return true;
        }else{
            return false;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOGGER.debug("Checkpoint successfully finished on flink with id: {}", checkpointId);
        updateProgress(checkpointId);
        if (currentRecordPendingCount < maxRecordPending) {
            unblockReader();
        }
    }

    @Override
    public List<DataPartition> snapshotState(long checkpointId) {
        LOGGER.debug("Storing snapshot for checkpoint with id: {}", checkpointId);
        this.currentCheckpointId = checkpointId;
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reader availability state: {} ", readerAvailable.state() == Future.State.SUCCESS ? "Not Blocked" : "Blocked");
        }
        return readerAvailable;
    }

    @Override
    public void addSplits(List<DataPartition> splits) {
        LOGGER.debug("Adding splits: {}", splits);
        currentSplits = splits;
        readerAvailable.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        LOGGER.debug("Notified that there are no more splits");
        noMoreSplits = true;
        unblockReader();
    }

    @Override
    public void close() throws Exception {
        dbConnectionProvider.close();
    }

    private void updateProgress(long checkpointId) {
        Set<Map.Entry<Long, Integer>> alreadyCommittedCheckpointSet = recordPendingCountPerCheckpoint.headMap(checkpointId, false).entrySet();

        long committedRecordPendingCount = alreadyCommittedCheckpointSet
                .stream().map(Map.Entry::getValue)
                .reduce(0, Integer::sum);
        Set<Long> committedCheckpoints = alreadyCommittedCheckpointSet
                .stream().map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        if (committedRecordPendingCount > 0) {
            LOGGER.debug("Storing progress for task with id: {} for checkpoints with ids: {} and total of records: {}", taskId, committedCheckpoints, committedRecordPendingCount);
            taskInfoRepository.incrementWriteCount(
                    taskId,
                    committedRecordPendingCount);
            allCommitedRecordCount += committedRecordPendingCount;
            currentSplitCommitedRecordCount += committedRecordPendingCount;
            currentRecordPendingCount -= committedRecordPendingCount;
            LOGGER.debug("Progress updated successfully! Increased commited records by: {}"
                    + ", commited in current split: {}, all commited: {}"
                , committedRecordPendingCount, currentSplitCommitedRecordCount, allCommitedRecordCount);
        } else {
            LOGGER.debug("Nothing to store for checkpoint with id: {} or less", checkpointId);
        }
        recordPendingCountPerCheckpoint.keySet().removeAll(committedCheckpoints);
    }

    private void blockReader() {
        LOGGER.debug("Blocking the reader");
        readerAvailable = new CompletableFuture<>();
    }

    private void unblockReader() {
        LOGGER.debug("Unblocking the reader - current pending: {}", currentRecordPendingCount);
        readerAvailable.complete(null);
    }

}
