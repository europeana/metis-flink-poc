package eu.europeana.cloud.flink.common.source;

import eu.europeana.cloud.flink.common.db.DbConnection;
import eu.europeana.cloud.flink.common.repository.ExecutionRecordRepository;
import eu.europeana.cloud.flink.common.repository.TaskInfoRepository;
import eu.europeana.cloud.flink.model.ExecutionRecord;
import eu.europeana.cloud.flink.model.ExecutionRecordKey;
import eu.europeana.cloud.flink.model.TaskInfo;
import eu.europeana.cloud.flink.model.TaskInfoNotFoundException;
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
        taskId = parameterTool.getLong("TASK_ID");
    }

    @Override
    public void start() {
        LOGGER.info("Starting source reader");
        executionRecordRepository = new ExecutionRecordRepository(new DbConnection(parameterTool));
        taskInfoRepository = new TaskInfoRepository(new DbConnection(parameterTool));
    }

    @Override
    public InputStatus pollNext(ReaderOutput<ExecutionRecord> output) throws Exception {
        LOGGER.info("Pooling next record");
        if (noMoreSplits) {
            LOGGER.info("There are no more splits");
            return InputStatus.END_OF_INPUT;
        }
        if (!splitFetched) {
            LOGGER.info("Fetching splits");
            context.sendSplitRequest();
            splitFetched = true;
        }
        if (!currentSplits.isEmpty()) {
            counter = 0;
            DataPartition currentSplit = currentSplits.removeFirst();
            try {
                ResultSet records = executionRecordRepository.getByDatasetIdAndExecutionIdAndOffsetAndLimit(
                        parameterTool.get("datasetId"),
                        parameterTool.get("executionId"),
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
                    LOGGER.info("Emitting record {}", executionRecord.getExecutionRecordKey().getRecordId());
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
            LOGGER.info("Got checkpoint after blockage {}", checkpointId);
            updateProgress();
            unblockReader();
        } else {
            LOGGER.info("Ignoring checkpoint");
        }
    }

    @Override
    public List<DataPartition> snapshotState(long checkpointId) {
        if (readerBlocked && this.checkpointId == 0) {
            LOGGER.info("Storing snapshotId={} for finished partition ", this.checkpointId);
            this.checkpointId = checkpointId;
        }
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.runAsync(() -> {
            while (true) {
                if (readerBlocked) {
                    LOGGER.debug("Waiting for reader to be unblocked");
                    try {
                        Thread.currentThread().sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    return;
                }
            }
        });
    }

    @Override
    public void addSplits(List<DataPartition> splits) {
        LOGGER.info("Adding splits: {}", splits);
        currentSplits = splits;
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() throws Exception {

    }

    private void updateProgress() throws TaskInfoNotFoundException {
        LOGGER.info("Storing task progress");
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
        readerBlocked = true;
    }

    private void unblockReader() {
        LOGGER.debug("Unblocking the reader");
        readerBlocked = false;
        checkpointId = 0;
    }

    private boolean receivedSnapshotIndicatingEndOfChunk(long receivedCheckpointId){
        return this.checkpointId == receivedCheckpointId;
    }
}
