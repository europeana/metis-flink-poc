package eu.europeana.cloud.job.validation;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.ValidationOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p><b>General description:</b></p>
 * <p>This is second iteration of validation job that removes last one difference with the Spring based solution.
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
 * <p><b>Progress tracking:</b></p>
 * <p>In this implementation progress tracking were moved to {@link eu.europeana.cloud.source.DbReaderWithProgressHandling}
 * During job startup {@link eu.europeana.cloud.source.DbEnumerator}
 * will read the status from the DB and resume the job from the first chunk that was not fully processed.
 * In this approach it is possible that some records will be reprocessed, but in the worse case there will be
 * Chunk_size records that have to be reprocessed.
 *</p>
 *
 * <p><b>Task identifiers</b></p>
 * <p>
 * Task identifier maybe provided in the task parameters. In this case job will try to resume the given job.
 * In other case (when task identifier is not provided in the parameters) it will be generated randomly;
 * </p>
 *
 * <p><b>How to run the job</b></p>
 * <p>All required parameters have to be provided in application args. In case of this job we have the following (example) arguments:
 *
 * <p>
 *     <ul>--datasetId 12</ul>
 *     <ul>--executionId 261</ul>
 *     <ul>--chunkSize 12</ul>
 *     <ul>--validationType VALIDATION_EXTERNAL</ul>
 *     <ul>--datasource.url jdbc:postgresql://localhost:5432/spring-batch-metis-poc</ul>
 *     <ul>--datasource.username admin</ul>
 *     <ul>--datasource.password admin</ul>
 * </p>
 */
public class ValidationJobWithPostgresMultiThreadedOperation extends MetisJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationJobWithPostgresMultiThreadedOperation.class);

    protected ValidationJobWithPostgresMultiThreadedOperation(String[] args) {
        super(args, ParameterTool.fromArgs(args).getRequired(JobParamName.VALIDATION_TYPE));
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", ValidationJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new ValidationJobWithPostgresMultiThreadedOperation(args).execute();
    }

    @Override
    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator() {
        return new ValidationOperator();
    }
}
