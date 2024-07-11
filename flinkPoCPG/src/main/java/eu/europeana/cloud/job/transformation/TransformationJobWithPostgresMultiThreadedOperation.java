package eu.europeana.cloud.job.transformation;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.TransformationOperator;
import eu.europeana.cloud.flink.client.constants.postgres.JobName;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Transformation job that uses checkpoints and reader blockage.</p>
 *
 * <p><b>How to run the job</b></p>
 * <p>All required parameters have to be provided in application args. In case of this job we have the following (example) arguments:
 *
 * <p>
 *     <ul>--datasetId 12</ul>
 *     <ul>--executionId 261</ul>
 *     <ul>--chunkSize 12</ul>
 *     <ul>--metisDatasetName idA_metisDatasetNameA</ul>
 *     <ul>--metisDatasetCountry Greece</ul>
 *     <ul>--metisDatasetLanguage el</ul>
 *     <ul>--metisXsltUrl https://metis-core-rest.test.eanadev.org/datasets/xslt/6204e5e2514e773e6745f7e9</ul>
 *     <ul>--datasource.url jdbc:postgresql://localhost:5432/spring-batch-metis-poc</ul>
 *     <ul>--datasource.username admin</ul>
 *     <ul>--datasource.password admin</ul>
 * </p>
 *
 */
public class TransformationJobWithPostgresMultiThreadedOperation extends MetisJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformationJobWithPostgresMultiThreadedOperation.class);

    protected TransformationJobWithPostgresMultiThreadedOperation(String[] args) {
        super(args, JobName.TRANSFORMATION);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", TransformationJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new TransformationJobWithPostgresMultiThreadedOperation(args).execute();
    }

    @Override
    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator() {
        return new TransformationOperator();
    }
}
