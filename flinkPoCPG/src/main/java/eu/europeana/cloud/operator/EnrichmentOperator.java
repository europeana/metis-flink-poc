package eu.europeana.cloud.operator;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.JobName;
import eu.europeana.cloud.tool.JobParamName;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class EnrichmentOperator extends ProcessFunction<ExecutionRecord, ExecutionRecordResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentOperator.class);

    private ParameterTool parameterTool;
    private transient EnrichmentWorker enrichmentWorker;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());

        String dereferenceURL = parameterTool.getRequired(JobParamName.DEREFERENCE_SERVICE_URL);
        String enrichmentEntityManagementUrl = parameterTool.getRequired(JobParamName.ENRICHMENT_ENTITY_MANAGEMENT_URL);
        String enrichmentEntityApiKey = parameterTool.getRequired(JobParamName.ENRICHMENT_ENTITY_API_KEY);
        String enrichmentEntityApiUrl = parameterTool.getRequired(JobParamName.ENRICHMENT_ENTITY_API_URL);

        final EnricherProvider enricherProvider = new EnricherProvider();
        enricherProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl, enrichmentEntityApiKey);
        final DereferencerProvider dereferencerProvider = new DereferencerProvider();
        dereferencerProvider.setDereferenceUrl(dereferenceURL);
        dereferencerProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl,
                enrichmentEntityApiKey);

        enrichmentWorker = new EnrichmentWorkerImpl(dereferencerProvider.create(), enricherProvider.create());
        LOGGER.debug("Created enrichment operator.");
    }


    @Override
    public void processElement(ExecutionRecord sourceExecutionRecord, ProcessFunction<ExecutionRecord, ExecutionRecordResult>.Context ctx, Collector<ExecutionRecordResult> out) throws Exception {
        ProcessedResult<String> enrichmentResult =
                enrichmentWorker.process(sourceExecutionRecord.getRecordData());
        if (enrichmentResult.getRecordStatus() != ProcessedResult.RecordStatus.CONTINUE) {
            String reportString = enrichmentResult.getReport().stream().map(Object::toString).collect(Collectors.joining("\n"));
            out.collect(
                    ExecutionRecordResult.from(
                            sourceExecutionRecord,
                            parameterTool.get(JobParamName.TASK_ID),
                            JobName.ENRICHMENT,
                            "",
                            reportString)
            );
        } else {
            out.collect(
                    ExecutionRecordResult.from(
                            sourceExecutionRecord,
                            parameterTool.get(JobParamName.TASK_ID),
                            JobName.ENRICHMENT,
                            enrichmentResult.getProcessedRecord(),
                            null)
            );
        }
    }
}
