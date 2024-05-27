package eu.europeana.cloud.flink.validation;

import eu.europeana.cloud.flink.common.db.DbConnection;
import eu.europeana.cloud.flink.common.operator.ValidationOperator;
import eu.europeana.cloud.flink.common.repository.TaskInfoRepository;
import eu.europeana.cloud.flink.common.source.DbSinkFunction;
import eu.europeana.cloud.flink.common.source.DbSource;
import eu.europeana.cloud.flink.model.TaskInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * <p><b>General description:</b></p>
 * <p>This is the simplest possible implementation that I did to get better understanding of
 * the internals of Apache Flink. Here we have single threaded job (parallelism = 1) that doesn't use state,
 * checkpoints and checkpoint barriers.</p>
 *
 * <p>Additionally this implementation is similar the the Spring Batch implementation done in other PoC.
 * The only one difference is in the {@link ValidationOperator}. Here we have single threaded processing, where,
 * in Spring Batch we have multiple thread. But the other aspect are the same:
 * <ul>
 *      <li>single thread, single instance data reader</li>
 *      <li>single thread single instance data writer</li>
 *      <li>chunks are processed one ofter another in the same order as taken from DB</li>
 * </ul>
 * We will try to tackle this one difference in the next implementation {@link ValidationJobWithPostgresMultiThreadedOperation}.
 * </p>
 *
 *
 * <p>Such approach makes progress tracking much more easier to implement. This is because all the records are processed
 * in the same order as they are read from DB.</p>
 *
 *
 *
 * <p><b>Progress tracking:</b></p>
 * <p>To be able to resume task after crash we have to track the progress. It is implemented here in similar way
 * as it is in Spring Batch jobs. After successful chunk processing, information about it is stored in the database
 * (in {@link DbSinkFunction}). During job startup {@link eu.europeana.cloud.flink.common.source.DbEnumerator}
 * reads the status from the DB and resume the job from the first chunk that was not fully processed.
 * In this approach it is possible that some records will be reprocessed, but in the worse case there will be
 * <i><b>Chunk_size</b></i> records that has to be reprocessed.
 *</p>
 *
 *
 * <p><b>Task identifiers:</b></p>
 * <p>
 * Task identifier maybe provided in the task parameters. In this case job will try to resume the given job.
 * In other case (when task identifier is not provided in the parameters) it will be generated randomly;
 * </p>
 *
 *
 * <p>This source code is not optimal (there are multiple duplications, not static constants, multiple log messages ...).
 * This is because the main aim was to get familiar with Flink itself.</p>
 *
 */
public class ValidationJobWithPostgresSingleThreaded {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationJobWithPostgresSingleThreaded.class);

    public static void main(String[] args) throws Exception {

        LOGGER.info("Starting ValidationJobWithPostgresSingleThreaded...");
        ParameterTool tool = ParameterTool.fromArgs(args);
        tool = generateTaskIdIfNeeded(tool);
        final StreamExecutionEnvironment env = prepareEnvironment(tool);

        env.fromSource(
                        new DbSource(tool), WatermarkStrategy.noWatermarks(), "dbSource"
                )
                .process(new ValidationOperator())
                .addSink(new DbSinkFunction());

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
        return env;
    }
}
