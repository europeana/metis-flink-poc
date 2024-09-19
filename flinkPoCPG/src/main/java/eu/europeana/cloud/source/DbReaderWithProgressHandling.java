package eu.europeana.cloud.source;

import eu.europeana.cloud.flink.client.constants.postgres.JobParam;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.retryable.RetryableMethodExecutor;
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
    private CompletableFuture<Void> readerAvailable = new CompletableFuture<>();
    private final int maxRecordPending;
    private int currentRecordPendingCount;
    private int allCommitedRecordCount;
    private int currentSplitCommitedRecordCount;
    private final TreeMap<Long, Integer> recordPendingCountPerCheckpoint = new TreeMap<>();
    private boolean splitFetched = false;
    private boolean noMoreSplits = false;
    private long currentCheckpointId = INITIAL_CHECKPOINT_ID;

    private List<ExecutionRecord> polledRecords = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DbReaderWithProgressHandling.class);

    private List<DataPartition> currentSplits = new ArrayList<>();
    private DbConnectionProvider dbConnectionProvider;
    private DataPartition currentSplit;
    private int currentSplitEmittedRecordCount;

    public DbReaderWithProgressHandling(
            SourceReaderContext context,
            ParameterTool parameterTool) {
        this.context = context;
        this.parameterTool = parameterTool;
        maxRecordPending = parameterTool.
                getInt(
                        JobParamName.MAX_RECORD_PENDING,
                        JobParam.DEFAULT_READER_MAX_RECORD_PENDING_COUNT);
    }

    @Override
    public void start() {
        LOGGER.info("Starting source reader");
        dbConnectionProvider = new DbConnectionProvider(parameterTool);

        //TODO Using retry proxy is maybe not optimal strategy in this case. This source implements asynchronous interface, so
        // we could do this retries in poolNext() method by returning InputStatus.NOTHING_AVAILABLE, wait a bit and notify
        // completable future to poll source again. Or simple wait a bit in pollNext() but only once per one retry.
        // In such cases we would less block checkpointing mechanism, which should work smoothly in case of infrastructure problems
        // and potential job restarts. And when we do not block we could do more retries or longer pauses.
        executionRecordRepository = RetryableMethodExecutor.createRetryProxy(new ExecutionRecordRepository(dbConnectionProvider));
    }

    @Override
    public InputStatus pollNext(ReaderOutput<ExecutionRecord> output) throws Exception {
        LOGGER.debug("Pooling next record");
        if (noMoreSplits) {
            LOGGER.info("There are no more splits");
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
                    new SplitCompletedEvent(currentCheckpointId, currentSplit, currentSplitEmittedRecordCount)
                );
                currentSplit = null;
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
        currentSplitEmittedRecordCount++;
    }

    private void fetchRecordsIfNeeded() throws IOException {
        currentSplit = currentSplits.getFirst();
        if (polledRecords == null) {
            LOGGER.debug("Fetching records from database");
            polledRecords = executionRecordRepository.getByDatasetIdAndExecutionIdAndOffsetAndLimit(
                    parameterTool.getRequired(JobParamName.DATASET_ID),
                    parameterTool.getRequired(JobParamName.EXECUTION_ID),
                    currentSplit.offset(), currentSplit.limit());

            if(Math.random()<0.333) {
                throw new RuntimeException("DBReader failed on fetching records!");
            }

            currentSplitCommitedRecordCount = 0;
            currentSplitEmittedRecordCount = 0;
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
        updatePendingRecordsState(checkpointId);
        if (currentRecordPendingCount < maxRecordPending) {
            unblockReader();
        }
    }

    @Override
    public List<DataPartition> snapshotState(long checkpointId) {
        LOGGER.info("Storing snapshot for checkpoint with id: {}", checkpointId);
        this.currentCheckpointId = checkpointId;

        if (currentSplit != null) {
            //TODO we could consider if we need to sent the event every time although it does not look as a big overhead.
            //Cause it is not every record but only every snapshot.
            context.sendSourceEventToCoordinator(
                new ProgressSnapshotEvent(currentCheckpointId, currentSplit, currentSplitEmittedRecordCount));
        }

        //TODO Validate if this method returns valid content. It could return partition with somehow stored progress for
        // current split. It could be used in case of failure when this split goes back to the enumerator and could be used
        // for more current and actual progress state storing in case of failure. Cause the progress send in event coudl be
        // somehow delayed in this case it is stored in the state so could be on time,
        // but it would go to th enumerator only in case of failure. So would be interpreted only after failure.
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

    private void updatePendingRecordsState(long completedCheckpointId) {
        Set<Map.Entry<Long, Integer>> alreadyCommittedCheckpointSet = recordPendingCountPerCheckpoint.headMap(completedCheckpointId, false).entrySet();

        int committedRecordPendingCount = alreadyCommittedCheckpointSet
                .stream().map(Map.Entry::getValue)
                .reduce(0, Integer::sum);
        Set<Long> committedCheckpoints = alreadyCommittedCheckpointSet
                .stream().map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        if (committedRecordPendingCount > 0) {
            allCommitedRecordCount += committedRecordPendingCount;
            currentSplitCommitedRecordCount += committedRecordPendingCount;
            currentRecordPendingCount -= committedRecordPendingCount;
            LOGGER.debug("Pending records state updated successfully! Increased commited records by: {}"
                    + ", commited in current split: {}, all commited: {}"
                , committedRecordPendingCount, currentSplitCommitedRecordCount, allCommitedRecordCount);
        } else {
            LOGGER.debug("Pending records state did not changed for checkpoint with id: {} or less", completedCheckpointId);
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
