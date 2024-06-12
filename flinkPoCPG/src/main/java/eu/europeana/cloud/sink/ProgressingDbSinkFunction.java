package eu.europeana.cloud.sink;

import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.repository.ExecutionRecordRepository;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.tool.DbConnection;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.exception.TaskInfoNotFoundException;
import eu.europeana.cloud.tool.JobParamName;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores processed records in the database and upgrades job progress if needed
 */
public class ProgressingDbSinkFunction extends RichSinkFunction<ExecutionRecordResult> {

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
        taskInfoRepository = new TaskInfoRepository(new DbConnection(parameterTool));
        executionRecordRepository =new ExecutionRecordRepository(new DbConnection(parameterTool));
        chunkSize = parameterTool.getInt(JobParamName.CHUNK_SIZE);
        taskId = parameterTool.getLong("TASK_ID");
    }

    @Override
    public void finish() throws Exception {
        LOGGER.info("Commiting last one part");
        TaskInfo taskInfo = taskInfoRepository.get(taskId);
        taskInfoRepository.update(new TaskInfo(taskInfo.taskId(), taskInfo.commitCount() + 1, counter));
    }

    @Override
    public void invoke(ExecutionRecordResult executionRecordResult, Context context) throws Exception {
        counter++;
        storeProcessedRecord(executionRecordResult);
        updateProgressIfNeeded();
        LOGGER.info("Writing element {} from thread {}", executionRecordResult.getExecutionRecord().getExecutionRecordKey().getRecordId(), Thread.currentThread().threadId());
    }

    private void storeProcessedRecord(ExecutionRecordResult record) {
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
