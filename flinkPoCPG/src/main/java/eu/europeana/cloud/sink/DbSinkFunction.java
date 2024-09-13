package eu.europeana.cloud.sink;

import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.repository.ExecutionRecordExceptionLogRepository;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.retryable.RetryableMethodExecutor;
import eu.europeana.cloud.tool.DbConnectionProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Stores processed records in the database
 */
public class DbSinkFunction extends RichSinkFunction<ExecutionRecordResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbSinkFunction.class);

    private ExecutionRecordRepository executionRecordRepository;
    private ExecutionRecordExceptionLogRepository executionRecordExceptionLogRepository;
    private DbConnectionProvider dbConnectionProvider;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
        dbConnectionProvider = new DbConnectionProvider(parameterTool);
        executionRecordRepository = RetryableMethodExecutor.createRetryProxy(new ExecutionRecordRepository(dbConnectionProvider));
        executionRecordExceptionLogRepository =
            RetryableMethodExecutor.createRetryProxy(new ExecutionRecordExceptionLogRepository(dbConnectionProvider));
        LOGGER.debug("Opening DbSinkFunction");
    }

    @Override
    public void close() throws Exception {
        dbConnectionProvider.close();
    }

    @Override
    public void invoke(ExecutionRecordResult executionRecordResult, Context context) throws Exception {
        if (recordProcessedSuccessfully(executionRecordResult)) {
            storeProcessedRecord(executionRecordResult);
        } else {
            storeExecutionRecordException(executionRecordResult);
        }
        LOGGER.info("Written element {}", executionRecordResult.getExecutionRecord().getExecutionRecordKey().getRecordId());
    }

    private boolean recordProcessedSuccessfully(ExecutionRecordResult executionRecordResult) {
        return StringUtils.isEmpty(executionRecordResult.getException());
    }

    private void storeProcessedRecord(ExecutionRecordResult executionRecordResult) throws IOException {
        executionRecordRepository.save(executionRecordResult);
    }

    private void storeExecutionRecordException(ExecutionRecordResult executionRecordResult) {
        executionRecordExceptionLogRepository.save(executionRecordResult);
    }

}
