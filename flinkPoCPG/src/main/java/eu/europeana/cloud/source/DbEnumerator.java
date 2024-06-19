package eu.europeana.cloud.source;

import eu.europeana.cloud.exception.TaskInfoNotFoundException;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnection;
import eu.europeana.cloud.tool.JobParamName;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DbEnumerator implements SplitEnumerator<DataPartition, DbEnumeratorState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbEnumerator.class);
    private static final int DEFAULT_CHUNK_SIZE = 1000;

    private final SplitEnumeratorContext<DataPartition> context;
    private final DbEnumeratorState state;
    private final ParameterTool parameterTool;
    private final int chunkSize;

    ExecutionRecordRepository executionRecordRepository;
    TaskInfoRepository taskInfoRepo;

    private final List<DataPartition> dataPartitions = new ArrayList<>();

    public DbEnumerator(
            SplitEnumeratorContext<DataPartition> context,
            DbEnumeratorState state, ParameterTool parameterTool) {
        this.context = context;
        this.state = state;
        this.parameterTool = parameterTool;
        this.chunkSize = parameterTool.getInt(JobParamName.CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
    }

    @Override
    public void start() {
        LOGGER.info("Starting DbEnumerator");
        executionRecordRepository = new ExecutionRecordRepository(new DbConnection(parameterTool));
        taskInfoRepo = new TaskInfoRepository(new DbConnection(parameterTool));
        prepareSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOGGER.info("Handling split request, subtaskId: {}, host: {}", subtaskId, requesterHostname);
        if (dataPartitions.isEmpty()) {
            LOGGER.info("No more splits");
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        DataPartition splitToBeServed = dataPartitions.removeFirst();
        context.assignSplit(splitToBeServed, subtaskId);
        LOGGER.info("Assigned split for subtaskId: {}, host: {}", subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<DataPartition> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {
        LOGGER.info("Adding reader for subtaskId: {}", subtaskId);
    }


    @Override
    public DbEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOGGER.info("Snapshot state checkpoint: {}", checkpointId);
        return state;
    }

    @Override
    public void close() throws IOException {
        executionRecordRepository.close();
        taskInfoRepo.close();
    }


    private void prepareSplits() {
        LOGGER.info("Preparing splits");
        try {
            TaskInfo taskInfo = taskInfoRepo.get(parameterTool.getLong(JobParamName.TASK_ID));
            long recordsToBeProcessed = executionRecordRepository.countByDatasetIdAndExecutionId(
                    parameterTool.getRequired(JobParamName.DATASET_ID),
                    parameterTool.getRequired(JobParamName.EXECUTION_ID));

            LOGGER.info("Records to be processed: {}", recordsToBeProcessed);
            for (long i = taskInfo.writeCount(); i < recordsToBeProcessed; i += chunkSize) {
                dataPartitions.add(new DataPartition(i, chunkSize));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (TaskInfoNotFoundException e) {
            LOGGER.error("Task not found in the database. It should never happen", e);
            System.exit(1);
        }
        LOGGER.info("Finished preparing splits: {}", dataPartitions);
    }
}
