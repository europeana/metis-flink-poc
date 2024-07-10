package eu.europeana.cloud.job.normalization;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.NormalizationOperator;
import eu.europeana.cloud.flink.client.constants.postgres.JobName;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p><b>How to run the job</b></p>
 * <p>All required parameters have to be provided in application args. In case of this job we have the following (example) arguments:
 *
 * <p>
 *     <ul>--datasetId 12</ul>
 *     <ul>--executionId 261</ul>
 *     <ul>--chunkSize 12</ul>
 *     <ul>--datasource.url jdbc:postgresql://localhost:5432/spring-batch-metis-poc</ul>
 *     <ul>--datasource.username admin</ul>
 *     <ul>--datasource.password admin</ul>
 * </p>
 */
public class NormalizationJobWithPostgresMultiThreadedOperation extends MetisJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationJobWithPostgresMultiThreadedOperation.class);

    protected NormalizationJobWithPostgresMultiThreadedOperation(String[] args) {
        super(args, JobName.NORMALIZATION);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", NormalizationJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new NormalizationJobWithPostgresMultiThreadedOperation(args).execute();
    }

    @Override
    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator() {
        return new NormalizationOperator();
    }
}
