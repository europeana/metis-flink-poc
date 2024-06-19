package eu.europeana.cloud.job.indexing;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.IndexingOperator;
import eu.europeana.cloud.tool.JobName;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingJobWithPostgresMultiThreadedOperation extends MetisJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingJobWithPostgresMultiThreadedOperation.class);

    protected IndexingJobWithPostgresMultiThreadedOperation(String[] args) {
        super(args, JobName.INDEXING);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", IndexingJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new IndexingJobWithPostgresMultiThreadedOperation(args).execute();
    }

    @Override
    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator() {
        return new IndexingOperator();
    }
}
