package eu.europeana.cloud.job.media;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.MediaOperator;
import eu.europeana.cloud.flink.client.constants.postgres.JobName;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MediaJobWithPostgresMultiThreadedOperation extends MetisJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediaJobWithPostgresMultiThreadedOperation.class);

    protected MediaJobWithPostgresMultiThreadedOperation(String[] args) {
        super(args, JobName.MEDIA);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", MediaJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new MediaJobWithPostgresMultiThreadedOperation(args).execute();
    }

    @Override
    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator() {
        return new MediaOperator();
    }
}
