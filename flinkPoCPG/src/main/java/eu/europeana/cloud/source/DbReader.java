package eu.europeana.cloud.source;

import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.model.DataPartition;
import eu.europeana.cloud.tool.DbConnectionProvider;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DbReader implements SourceReader<ExecutionRecord, DataPartition> {


    private final SourceReaderContext context;
    private final ParameterTool parameterTool;
    private ExecutionRecordRepository executionRecordRepository;
    private boolean splitFetched = false;
    private boolean noMoreSplits = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(DbReader.class);

    private List<DataPartition> currentSplits = new ArrayList<>();
    private DbConnectionProvider dbConnectionProvider;

    public DbReader(
            SourceReaderContext context,
            ParameterTool parameterTool) {
        this.context = context;
        this.parameterTool = parameterTool;
    }

    @Override
    public void start() {
        LOGGER.info("Starting source reader");
        dbConnectionProvider = new DbConnectionProvider(parameterTool);
        this.executionRecordRepository = new ExecutionRecordRepository(dbConnectionProvider);
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
            DataPartition currentSplit = currentSplits.removeFirst();

            List<ExecutionRecord> results = executionRecordRepository.getByDatasetIdAndExecutionIdAndOffsetAndLimit(
                    parameterTool.getRequired(JobParamName.DATASET_ID),
                    parameterTool.getRequired(JobParamName.EXECUTION_ID),
                    currentSplit.offset(), currentSplit.limit());

            results.forEach(result -> {
                LOGGER.info("Emitting record {}", result.getExecutionRecordKey().getRecordId());
                output.collect(result);
            });
            currentSplits = new ArrayList<>();
            splitFetched = false;
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<DataPartition> snapshotState(long checkpointId) {
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.runAsync(() -> {
            try {
                /*This is done by purpose to make the processing a bit slower to be able to debug it easier.
                * This sleep will be removed in the future */
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
        dbConnectionProvider.close();
    }
}
