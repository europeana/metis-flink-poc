package eu.europeana.cloud.tool.validation;

import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.java.utils.ParameterTool;

public class EnrichmentJobParamValidator implements JobParamValidator{

    @Override
    public void validate(ParameterTool parameterTool) {

        parameterTool.getRequired(JobParamName.DATASET_ID);
        parameterTool.getRequired(JobParamName.EXECUTION_ID);
        parameterTool.getRequired(JobParamName.DEREFERENCE_SERVICE_URL);
        parameterTool.getRequired(JobParamName.ENRICHMENT_ENTITY_MANAGEMENT_URL);
        parameterTool.getRequired(JobParamName.ENRICHMENT_ENTITY_API_KEY);
        parameterTool.getRequired(JobParamName.ENRICHMENT_ENTITY_API_URL);
    }
}
