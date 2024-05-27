package eu.europeana.cloud.flink.validation;

import eu.europeana.cloud.flink.common.db.DbConnection;
import eu.europeana.cloud.flink.common.operator.ValidationOperator;
import eu.europeana.cloud.flink.common.repository.TaskInfoRepository;
import eu.europeana.cloud.flink.common.source.DbSinkFunction;
import eu.europeana.cloud.flink.common.source.DbSourceWithProgressHandling;
import eu.europeana.cloud.flink.model.TaskInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * This job is the same as {@link ValidationJobWithPostgresMultiThreadedOperation} but with multithreaded sink.
 */
public class ValidationJobWithPostgresMultiThreadedOperationAndSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationJobWithPostgresMultiThreadedOperationAndSink.class);

    private static final int DEFAULT_PARALLELISM = 4;

    public static void main(String[] args) throws Exception {

        LOGGER.info("Starting ValidationJobWithPostgresSingleThreaded...");
        ParameterTool tool = ParameterTool.fromArgs(args);
        tool = generateTaskIdIfNeeded(tool);

        final StreamExecutionEnvironment env = prepareEnvironment(tool);

        env.fromSource(
                        new DbSourceWithProgressHandling(tool), WatermarkStrategy.noWatermarks(), "dbSource"
                ).setParallelism(1)
                .process(new ValidationOperator()).setParallelism(tool.getInt("PARALLELISM", DEFAULT_PARALLELISM))
                .addSink(new DbSinkFunction()).setParallelism(tool.getInt("PARALLELISM", DEFAULT_PARALLELISM));

        env.execute();
    }

    private static ParameterTool generateTaskIdIfNeeded(ParameterTool tool) {
        try (TaskInfoRepository taskInfoRepository = new TaskInfoRepository(new DbConnection(tool))) {
            if (tool.get("TASK_ID") == null) {
                long taskId = new Random().nextLong();
                taskInfoRepository.save(new TaskInfo(taskId, 0L, 0L));
                tool = tool.mergeWith(ParameterTool.fromMap(Map.of("TASK_ID", taskId + "")));
            }
            return tool;
        }
    }

    protected static StreamExecutionEnvironment prepareEnvironment(ParameterTool tool) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.getConfig().setGlobalJobParameters(tool);
        env.enableCheckpointing(5000);
        return env;
    }
}
