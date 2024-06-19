package eu.europeana.cloud.common;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.sink.DbSinkFunction;
import eu.europeana.cloud.source.DbSourceWithProgressHandling;
import eu.europeana.cloud.tool.DbConnection;
import eu.europeana.cloud.tool.JobParam;
import eu.europeana.cloud.tool.JobParamName;
import eu.europeana.cloud.tool.validation.JobParamValidatorFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.util.Map;
import java.util.Random;

public abstract class MetisJob {

    protected final StreamExecutionEnvironment flinkEnvironment;
    protected String jobName;
    protected ParameterTool tool;
    private final Random taskIdGenerator = new Random();

    protected MetisJob(String[] args, String jobName) {
        this.jobName = jobName;
        tool = ParameterTool.fromArgs(args);
        flinkEnvironment = prepareEnvironment();
    }

    private StreamExecutionEnvironment prepareEnvironment() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        generateTaskIdIfNeeded();
        env.getConfig().setGlobalJobParameters(tool);
        env.enableCheckpointing(2000);
        return env;
    }

    private void validateJobParams() {
        JobParamValidatorFactory.getValidator(jobName).validate(tool);
    }

    private void generateTaskIdIfNeeded() {
        try (TaskInfoRepository taskInfoRepository = new TaskInfoRepository(new DbConnection(tool))) {
            if (tool.get(JobParamName.TASK_ID) == null) {
                long taskId = taskIdGenerator.nextLong();
                taskInfoRepository.save(new TaskInfo(taskId, 0L, 0L));
                tool = tool.mergeWith(ParameterTool.fromMap(Map.of(JobParamName.TASK_ID, taskId + "")));
            }
        }
    }

    private void prepareJob() {
        flinkEnvironment.fromSource(
                        new DbSourceWithProgressHandling(tool), WatermarkStrategy.noWatermarks(), "dbSource"
                ).setParallelism(1)
                .process(getMainOperator()).setParallelism(
                        tool.getInt(
                                JobParamName.OPERATOR_PARALLELISM,
                                JobParam.DEFAULT_OPERATOR_PARALLELISM))
                .addSink(new DbSinkFunction()).setParallelism(1);
    }

    public void execute() throws Exception {
        validateJobParams();
        prepareJob();
        flinkEnvironment.execute();
    }

    public abstract ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator();

}
