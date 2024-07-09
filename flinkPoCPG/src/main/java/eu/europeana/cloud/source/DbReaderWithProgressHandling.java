package eu.europeana.cloud.source;

import eu.europeana.cloud.exception.TaskInfoNotFoundException;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordKey;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnection;
import eu.europeana.cloud.tool.JobParamName;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class DbReaderWithProgressHandling implements SourceReader<ExecutionRecord, DataPartition> {

    private final SourceReaderContext context;
    private final ParameterTool parameterTool;
    private ExecutionRecordRepository executionRecordRepository;
    private TaskInfoRepository taskInfoRepository;
    private CompletableFuture<Void> readerAvailable = new CompletableFuture<>();
    private final int maxRecordPending;
    private int currentRecordPendingCount;
    private final TreeMap<Long, Integer> recordPendingCountPerCheckpoint = new TreeMap<>();
    private boolean splitFetched = false;
    private boolean noMoreSplits = false;
    private long checkpointId = -1;
    private final long taskId;

    private ResultSet polledRecords = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DbReaderWithProgressHandling.class);

    private List<DataPartition> currentSplits = new ArrayList<>();

    public DbReaderWithProgressHandling(
            SourceReaderContext context,
            ParameterTool parameterTool) {
        this.context = context;
        this.parameterTool = parameterTool;
        taskId = parameterTool.getLong(JobParamName.TASK_ID);
        maxRecordPending = parameterTool.has(JobParamName.MAX_RECORD_PENDING) ? parameterTool.getInt(JobParamName.MAX_RECORD_PENDING) : 100;
    }

    @Override
    public void start() {
        LOGGER.info("Starting source reader");
        executionRecordRepository = new ExecutionRecordRepository(new DbConnection(parameterTool));
        taskInfoRepository = new TaskInfoRepository(new DbConnection(parameterTool));
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
            DataPartition currentSplit = currentSplits.getFirst();
            if (polledRecords == null) {
                LOGGER.debug("Fetching records from database");
                try {
                    polledRecords = executionRecordRepository.getByDatasetIdAndExecutionIdAndOffsetAndLimit(
                            parameterTool.get(JobParamName.DATASET_ID),
                            parameterTool.get(JobParamName.EXECUTION_ID),
                            currentSplit.offset(), currentSplit.limit());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            } else {
                LOGGER.debug("Already fetched records exist");
            }

            boolean isResultSetProcessed = true;
            while (polledRecords.next()) {
                ExecutionRecord executionRecord = ExecutionRecord.builder()
                        .executionRecordKey(
                                ExecutionRecordKey.builder()
                                        .datasetId(polledRecords.getString("dataset_id"))
                                        .executionId(polledRecords.getString("execution_id"))
                                        .recordId(polledRecords.getString("record_id"))
                                        .build())
                        .executionName(polledRecords.getString("execution_name"))
                        .recordData(new String(polledRecords.getBytes("record_data")))
                        .build();
                currentRecordPendingCount++;
                int currentlyPendingForThisCheckpoint = 1;
                if (recordPendingCountPerCheckpoint.containsKey(checkpointId)) {
                    currentlyPendingForThisCheckpoint = recordPendingCountPerCheckpoint.get(checkpointId);
                    recordPendingCountPerCheckpoint.put(checkpointId, ++currentlyPendingForThisCheckpoint);
                } else {
                    recordPendingCountPerCheckpoint.put(checkpointId, currentlyPendingForThisCheckpoint);
                }
                LOGGER.debug("Emitting record {} - currently pending {} records", executionRecord.getExecutionRecordKey().getRecordId(), currentRecordPendingCount);
                LOGGER.debug("There are {} records pending for checkpoint {}", currentlyPendingForThisCheckpoint, checkpointId);
                output.collect(executionRecord);
                if (currentRecordPendingCount >= maxRecordPending) {
                    LOGGER.debug("Blocking reader due to hitting pending records limit");
                    blockReader();
                    isResultSetProcessed = false;
                    break;
                }
            }
            if (isResultSetProcessed) {
                LOGGER.debug("Removing split due to exhaustion of polled result set");
                currentSplits.removeFirst();
                splitFetched = false;
                polledRecords = null;
                return InputStatus.MORE_AVAILABLE;
            } else {
                LOGGER.debug("Sending signal about more records being present in polled result set");
            }
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOGGER.debug("Checkpoint successfully finished on flink with id: {}", checkpointId);
        updateProgress(checkpointId);
        if (currentRecordPendingCount < maxRecordPending) {
            unblockReader();
        }
    }

    @Override
    public List<DataPartition> snapshotState(long checkpointId) {
        LOGGER.debug("Storing snapshot for checkpoint with id: {}", this.checkpointId);
        this.checkpointId = checkpointId;
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        LOGGER.debug("Reader availability state: {} ", readerAvailable.state());
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

    }

    private void updateProgress(long checkpointId) throws TaskInfoNotFoundException {
        NavigableMap<Long, Integer> recordsAlreadyProcessed = recordPendingCountPerCheckpoint.headMap(checkpointId, false);
        for (Map.Entry<Long, Integer> entry : recordsAlreadyProcessed.entrySet()) {
            Long currentlyProcessedCheckpointId = entry.getKey();
            Integer pendingRecordCount = entry.getValue();
            LOGGER.debug("Storing task progress for given checkpoint: {} records: {}", currentlyProcessedCheckpointId, pendingRecordCount);
            TaskInfo taskInfo = taskInfoRepository.get(taskId);
            taskInfoRepository.update(
                    new TaskInfo(
                            taskInfo.taskId(),
                            taskInfo.commitCount() + 1,
                            taskInfo.writeCount() + pendingRecordCount));
            currentRecordPendingCount -= pendingRecordCount;
            recordPendingCountPerCheckpoint.remove(currentlyProcessedCheckpointId);
        }
        if (recordsAlreadyProcessed.isEmpty()) {
            LOGGER.debug("Nothing to store for checkpoint: {} or less", checkpointId);
        }
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
