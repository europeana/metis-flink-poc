package eu.europeana.cloud.flink.common.source;

import eu.europeana.cloud.flink.common.db.DbConnection;
import eu.europeana.cloud.flink.common.repository.ExecutionRecordRepository;
import eu.europeana.cloud.flink.common.repository.TaskInfoRepository;
import eu.europeana.cloud.flink.model.ExecutionRecord;
import eu.europeana.cloud.flink.model.TaskInfo;
import eu.europeana.cloud.flink.model.TaskInfoNotFoundException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores processed records in the database and upgrades job progress if needed
 */
public class ProgressingDbSinkFunction extends RichSinkFunction<ExecutionRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressingDbSinkFunction.class);

    private TaskInfoRepository taskInfoRepository;
    private ExecutionRecordRepository executionRecordRepository;
    private int chunkSize;
    private int counter = 0;
    private long taskId;

    @Override
    public void open(Configuration parameters) throws Exception {

        LOGGER.info("Opening DbSinkFunction");

        ParameterTool parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
        this.taskInfoRepository = new TaskInfoRepository(new DbConnection(parameterTool));
        this.executionRecordRepository =new ExecutionRecordRepository(new DbConnection(parameterTool));
        this.chunkSize = parameterTool.getInt("CHUNK_SIZE");
        taskId = parameterTool.getLong("TASK_ID");
    }

    @Override
    public void finish() throws Exception {
        LOGGER.info("Commiting last one part");
        TaskInfo taskInfo = taskInfoRepository.get(taskId);
        taskInfoRepository.update(new TaskInfo(taskInfo.taskId(), taskInfo.commitCount() + 1, counter));
    }

    @Override
    public void invoke(ExecutionRecord element, Context context) throws Exception {
        counter++;
        storeProcessedRecord(element);
        updateProgressIfNeeded();
        LOGGER.info("Writing element {} from thread {}", element.getExecutionRecordKey().getRecordId(), Thread.currentThread().threadId());
    }

    private void storeProcessedRecord(ExecutionRecord record) {
        executionRecordRepository.save(record);
    }

    private void updateProgressIfNeeded() throws TaskInfoNotFoundException {
        if (counter % chunkSize == 0) {

            LOGGER.info("Commiting progress");
            TaskInfo taskInfo = taskInfoRepository.get(taskId);
            taskId = taskInfo.taskId();
            taskInfoRepository.update(new TaskInfo(taskInfo.taskId(), taskInfo.commitCount() + 1, counter));
        }
    }
}
