package eu.europeana.cloud.source;

import eu.europeana.cloud.exception.TaskInfoNotFoundException;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnectionProvider;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import java.util.Optional;
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

    private final SplitEnumeratorContext<DataPartition> context;
    private final ParameterTool parameterTool;
    private final int chunkSize;

    ExecutionRecordRepository executionRecordRepository;
    TaskInfoRepository taskInfoRepo;

    private List<DataPartition> dataPartitions;
    private List<DataPartition> executingPartitions = new ArrayList<>();
    private DbConnectionProvider dbConnectionProvider;

    public DbEnumerator(
            SplitEnumeratorContext<DataPartition> context,
            DbEnumeratorState state, ParameterTool parameterTool) {
        this.context = context;
        this.parameterTool = parameterTool;
        this.chunkSize = parameterTool.getInt(JobParamName.CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
        this.dataPartitions = Optional.ofNullable(state).map(DbEnumeratorState::getPartitions).orElse(null);

        LOGGER.info("Created DbEnumerator with {} partitions.",dataPartitions!=null?dataPartitions.size():"no fetched");
    }

    @Override
    public void start() {
        LOGGER.info("Starting DbEnumerator");
        dbConnectionProvider = new DbConnectionProvider(parameterTool);
        executionRecordRepository = new ExecutionRecordRepository(dbConnectionProvider);
        taskInfoRepo = new TaskInfoRepository(dbConnectionProvider);
        if (dataPartitions == null) {
            prepareSplits();
        }else{
            LOGGER.info("Splits are already initialized count: {}", dataPartitions.size());
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOGGER.info("Handling split request, subtaskId: {}, host: {}", subtaskId, requesterHostname);
        if (dataPartitions.isEmpty()) {
            LOGGER.info("No more remaining splits, {} splits executing!", executingPartitions.size());
            context.signalNoMoreSplits(subtaskId);
            return;
        }

        //TODO Removing first element from big ArrayList could be performance problem cause needs shifting the whole array
        //We should remove last and reverse list order at least. ut in practice is good to do it more optimally
        DataPartition splitToBeServed = dataPartitions.removeFirst();
        executingPartitions.add(splitToBeServed);
        context.assignSplit(splitToBeServed, subtaskId);
        LOGGER.info("Assigned split: {} for subtaskId: {}, host: {}, executing: {} splits, {} left.",
            splitToBeServed, subtaskId, requesterHostname, executingPartitions.size(), dataPartitions.size());
    }

    @Override
    public void addSplitsBack(List<DataPartition> splits, int subtaskId) {
        dataPartitions.addAll(splits);
        LOGGER.info("Added splits from the subtask: {} back: {}, executing: {} splits, {} left.",
            subtaskId, splits, executingPartitions.size(), dataPartitions.size());
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if(sourceEvent instanceof SplitCompletedEvent event){
            boolean removed = executingPartitions.remove(event.getCurrentSplit());
            LOGGER.info("Removed - {}, completed split: {}, executing {} splits, {} left.",
                removed, event.getCurrentSplit(), executingPartitions.size(), dataPartitions.size());
        }

    }

    @Override
    public void addReader(int subtaskId) {
        LOGGER.info("New reader added for, the subtaskId: {}", subtaskId);
    }


    @Override
    public DbEnumeratorState snapshotState(long checkpointId) throws Exception {
        //TODO We need to clone list, what could be performance and expecially memory overhead for big dataset.
        //We could consider to not store the whole list but for example store number of assigned
        //partitions and only store returned partitions on the list. Which should be very small.
        ArrayList<DataPartition> partitions = new ArrayList<>();
        partitions.addAll(executingPartitions);
        partitions.addAll(dataPartitions);
        DbEnumeratorState state=DbEnumeratorState.builder()
                                                 .partitions(partitions).build();
        LOGGER.info("Snapshot state with: {} partitions, for the checkpoint: {}", state.getPartitions().size(), checkpointId);
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOGGER.info("Checkpoint completed: {}", checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        LOGGER.info("Checkpoint aborted: {}!",  checkpointId);
    }

    @Override
    public void close() throws IOException {
        try {
            dbConnectionProvider.close();
        }catch (Exception e){
            throw new IOException("Could not close dbProvider!",e);
        }
    }


    private void prepareSplits() {
        LOGGER.info("Preparing splits");
        try {
            TaskInfo taskInfo = taskInfoRepo.get(parameterTool.getLong(JobParamName.TASK_ID));
            long recordsToBeProcessed = executionRecordRepository.countByDatasetIdAndExecutionId(
                    parameterTool.getRequired(JobParamName.DATASET_ID),
                    parameterTool.getRequired(JobParamName.EXECUTION_ID));

            LOGGER.info("Records to be processed: {}", recordsToBeProcessed);
            dataPartitions = new ArrayList<>();
            for (long i = 0; i < recordsToBeProcessed; i += chunkSize) {
                dataPartitions.add(new DataPartition(i, chunkSize));
            }
            LOGGER.info("Prepared {} splits.", dataPartitions.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TaskInfoNotFoundException e) {
            LOGGER.error("Task not found in the database. It should never happen", e);
            System.exit(1);
        }
        LOGGER.info("Finished preparing splits: {}", dataPartitions);
    }
}
