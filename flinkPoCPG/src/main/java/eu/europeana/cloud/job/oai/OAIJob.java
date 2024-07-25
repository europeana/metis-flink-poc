package eu.europeana.cloud.job.oai;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.DeletedRecordFilter;
import eu.europeana.cloud.operator.IdAssigningOperator;
import eu.europeana.cloud.operator.RecordHarvestingOperator;
import eu.europeana.cloud.source.oai.OAIHeadersSource;

import eu.europeana.cloud.sink.DbSinkFunction;
import eu.europeana.cloud.flink.client.constants.postgres.JobName;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIJob extends MetisJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIJob.class);

    protected OAIJob(String[] args) {
        super(args, JobName.OAI_HARVEST);
    }

    protected StreamExecutionEnvironment prepareEnvironment() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        generateTaskIdIfNeeded();
        env.getConfig().setGlobalJobParameters(tool);
        return env;
    }


    @Override
    protected void prepareJob() {
        flinkEnvironment.fromSource(
            new OAIHeadersSource(tool), WatermarkStrategy.noWatermarks(), createSourceName()).setParallelism(1)

        .filter(new DeletedRecordFilter()).setParallelism(operatorParallelism)
        .process(new RecordHarvestingOperator(tool)).setParallelism(operatorParallelism)
        .process(new IdAssigningOperator()).setParallelism(operatorParallelism)
                        .addSink(new DbSinkFunction()).setParallelism(sinkParallelism);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", OAIJob.class.getSimpleName());
        new OAIJob(args).execute();
    }

    private String createSourceName() {
        return "OAI (url: " + tool.get(JobParamName.OAI_REPOSITORY_URL)
            + ", set: " + tool.get(JobParamName.SET_SPEC) +
            ", format: " + tool.get(JobParamName.METADATA_PREFIX) + ")";
    }

    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator(){
        throw new UnsupportedOperationException();
    }
}
