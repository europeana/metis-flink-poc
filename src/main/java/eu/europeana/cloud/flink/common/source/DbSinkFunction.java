package eu.europeana.cloud.flink.common.source;

import eu.europeana.cloud.flink.common.db.DbConnection;
import eu.europeana.cloud.flink.common.repository.ExecutionRecordRepository;
import eu.europeana.cloud.flink.model.ExecutionRecord;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores processed records in the database
 */
public class DbSinkFunction extends RichSinkFunction<ExecutionRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbSinkFunction.class);

    private ExecutionRecordRepository executionRecordRepository;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
        this.executionRecordRepository =new ExecutionRecordRepository(new DbConnection(parameterTool));
        LOGGER.info("Opening DbSinkFunction");
    }

    @Override
    public void invoke(ExecutionRecord element, Context context) throws Exception {
        storeProcessedRecord(element);
        LOGGER.info("Writing element {}", element.getExecutionRecordKey().getRecordId());
    }

    private void storeProcessedRecord(ExecutionRecord executionRecord) {
        executionRecordRepository.save(executionRecord);
    }

}
