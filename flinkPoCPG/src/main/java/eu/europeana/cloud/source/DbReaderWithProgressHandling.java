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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DbReaderWithProgressHandling implements SourceReader<ExecutionRecord, DataPartition> {

    private final SourceReaderContext context;
    private final ParameterTool parameterTool;
    private ExecutionRecordRepository executionRecordRepository;
    private TaskInfoRepository taskInfoRepository;
    private CompletableFuture<Void> readerAvailable = new CompletableFuture<>();
    private boolean splitFetched = false;
    private boolean noMoreSplits = false;
    private boolean readerBlocked = false;
    private long checkpointId;
    private long taskId;
    private int counter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(DbReaderWithProgressHandling.class);

    private List<DataPartition> currentSplits = new ArrayList<>();

    public DbReaderWithProgressHandling(
            SourceReaderContext context,
            ParameterTool parameterTool) {
        this.context = context;
        this.parameterTool = parameterTool;
        taskId = parameterTool.getLong(JobParamName.TASK_ID);
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
            counter = 0;
            DataPartition currentSplit = currentSplits.removeFirst();
            try {
                ResultSet records = executionRecordRepository.getByDatasetIdAndExecutionIdAndOffsetAndLimit(
                        parameterTool.get(JobParamName.DATASET_ID),
                        parameterTool.get(JobParamName.EXECUTION_ID),
                        currentSplit.offset(), currentSplit.limit());
                while (records.next()) {
                    ExecutionRecord executionRecord = ExecutionRecord.builder()
                            .executionRecordKey(
                                    ExecutionRecordKey.builder()
                                            .datasetId(records.getString("dataset_id"))
                                            .executionId(records.getString("execution_id"))
                                            .recordId(records.getString("record_id"))
                                            .build())
                            .executionName(records.getString("execution_name"))
                            .recordData(new String(records.getBytes("record_data")))
                            .build();
                    LOGGER.debug("Emitting record {}", executionRecord.getExecutionRecordKey().getRecordId());
                    counter++;
                    output.collect(executionRecord);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            splitFetched = false;
            blockReader();
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (receivedSnapshotIndicatingEndOfChunk(checkpointId)) {
            LOGGER.debug("Got checkpoint after blockage {}", checkpointId);
            updateProgress();
            unblockReader();
        } else {
            LOGGER.debug("Ignoring checkpoint");
        }
    }

    @Override
    public List<DataPartition> snapshotState(long checkpointId) {
        if (readerBlocked && this.checkpointId == 0) {
            LOGGER.debug("Storing snapshotId={} for finished partition ", this.checkpointId);
            this.checkpointId = checkpointId;
        }
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
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

    private void updateProgress() throws TaskInfoNotFoundException {
        LOGGER.debug("Storing task progress");
        TaskInfo taskInfo = taskInfoRepository.get(taskId);
        taskId = taskInfo.taskId();
        taskInfoRepository.update(
                new TaskInfo(
                        taskInfo.taskId(),
                        taskInfo.commitCount() + 1,
                        taskInfo.writeCount() + counter));
    }

    private void blockReader() {
        LOGGER.debug("Blocking the reader");
        readerAvailable = new CompletableFuture<>();
        readerBlocked = true;
    }

    private void unblockReader() {
        LOGGER.debug("Unblocking the reader");
        readerAvailable.complete(null);
        readerBlocked = false;
        checkpointId = 0;
    }

    private boolean receivedSnapshotIndicatingEndOfChunk(long receivedCheckpointId) {
        return checkpointId != 0 && this.checkpointId <= receivedCheckpointId;
    }
}
