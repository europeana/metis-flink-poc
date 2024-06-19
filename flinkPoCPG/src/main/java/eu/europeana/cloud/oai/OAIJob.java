package eu.europeana.cloud.oai;

import eu.europeana.cloud.job.normalization.NormalizationJobWithPostgresMultiThreadedOperation;
import eu.europeana.cloud.oai.harvest.DeletedRecordFilter;
import eu.europeana.cloud.oai.harvest.IdAssigningOperator;
import eu.europeana.cloud.oai.harvest.RecordHarvestingOperator;
import eu.europeana.cloud.oai.source.OAIHeadersSource;

import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.repository.TaskInfoRepository;
import eu.europeana.cloud.sink.DbSinkFunction;
import eu.europeana.cloud.tool.DbConnection;
import eu.europeana.cloud.tool.JobName;
import eu.europeana.cloud.tool.JobParam;
import eu.europeana.cloud.tool.JobParamName;
import eu.europeana.cloud.tool.validation.JobParamValidatorFactory;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIJob.class);

    protected final StreamExecutionEnvironment flinkEnvironment;
    protected ParameterTool tool;
    private final Random taskIdGenerator = new Random();

    protected OAIJob(String[] args) {
        tool = ParameterTool.fromArgs(args);
        flinkEnvironment = prepareEnvironment();
    }

    private StreamExecutionEnvironment prepareEnvironment() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        generateTaskIdIfNeeded();
        env.getConfig().setGlobalJobParameters(tool);
        return env;
    }

    private void validateJobParams() {
        JobParamValidatorFactory.getValidator(JobName.OAI_HARVEST).validate(tool);
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
        int parallelism = tool.getInt(
            JobParamName.OPERATOR_PARALLELISM,
            JobParam.DEFAULT_OPERATOR_PARALLELISM);
        flinkEnvironment.fromSource(
            new OAIHeadersSource(tool), WatermarkStrategy.noWatermarks(), "OAI Source").setParallelism(1)

        .filter(new DeletedRecordFilter()).setParallelism(parallelism)
        .process(new RecordHarvestingOperator(tool)).setParallelism(parallelism)
        .process(new IdAssigningOperator(tool)).setParallelism(parallelism)
                        .addSink(new DbSinkFunction()).setParallelism(1);
    }

    public void execute() throws Exception {
        validateJobParams();
        prepareJob();
        flinkEnvironment.execute();
    }


    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", NormalizationJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new OAIJob(args).execute();
    }

}
