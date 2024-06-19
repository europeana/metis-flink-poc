package eu.europeana.cloud.flink.validation;

import eu.europeana.cloud.flink.common.db.DbConnection;
import eu.europeana.cloud.flink.common.operator.ValidationOperator;
import eu.europeana.cloud.flink.common.repository.TaskInfoRepository;
import eu.europeana.cloud.flink.common.source.ProgressingDbSinkFunction;
import eu.europeana.cloud.flink.common.source.DbSourceWithProgressHandling;
import eu.europeana.cloud.flink.common.source.DbSinkFunction;
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
 * <p>This is second iteration of validation job that removes last one difference with the Sping based solution.
 * Now we have multithreaded operator that does the validation. It means that we have the following solution:
 *
 * <ul>
 *      <li>single thread, single instance data reader</li>
 *      <li>multithreaded validation operator (equivalent of validation processor from Spring PoC)</li>
 *      <li>single thread single instance data writer</li>
 *      <li>chunks are processed one ofter another in the same order as taken from DB</li>
 * </ul>
 *
 * <p>Such approach makes progress tracking more difficult to implement. To do that I used Flink stream barriers
 * (special messages that are injected to the stream and flow with the records. Barriers never overtake records)
 * and blockage of the reader.
 * The algorithm is the following:
 *  - after emitting last one records from the chunk (from split) we block the reader;
 *  - reader keeps the checkpointId of the first checkpoint emitted after be blockage;
 *  - reader waits for the completion of the given checkpoint;
 *  - when the checkpoint is completed, reader updates the progress and unblocks the processing;
 *
 *
 * <p>Progress tracking:</p>
 * In this implementation progress tracking were moved from {@link ProgressingDbSinkFunction} to {@link eu.europeana.cloud.flink.common.source.DbReaderWithProgressHandling}
 * During job startup {@link eu.europeana.cloud.flink.common.source.DbEnumerator}
 * will read the status from the DB and resume the job from the first chunk that was not fully processed.
 * In this approach it is possible that some records will be reprocessed, but in the worse case there will be
 * Chunk_size records that have to be reprocessed.
 *
 *
 * <p>Task identifiers</p>
 * Task identifier maybe provided in the task parameters. In this case job will try to resume the given job.
 * In other case (when task identifier is not provided in the parameters) it will be generated randomly;
 *
 */
@Deprecated
public class ValidationJobWithPostgresMultiThreadedOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationJobWithPostgresMultiThreadedOperation.class);

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
                .addSink(new DbSinkFunction()).setParallelism(1);

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
