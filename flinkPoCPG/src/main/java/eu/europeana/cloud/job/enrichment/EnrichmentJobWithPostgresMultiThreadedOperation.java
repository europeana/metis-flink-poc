package eu.europeana.cloud.job.enrichment;

import eu.europeana.cloud.common.MetisJob;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.operator.EnrichmentOperator;
import eu.europeana.cloud.tool.JobName;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentJobWithPostgresMultiThreadedOperation extends MetisJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentJobWithPostgresMultiThreadedOperation.class);

    protected EnrichmentJobWithPostgresMultiThreadedOperation(String[] args) {
        super(args, JobName.ENRICHMENT);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting {}...", EnrichmentJobWithPostgresMultiThreadedOperation.class.getSimpleName());
        new EnrichmentJobWithPostgresMultiThreadedOperation(args).execute();
    }

    @Override
    public ProcessFunction<ExecutionRecord, ExecutionRecordResult> getMainOperator() {
        return new EnrichmentOperator();
    }
}
