package eu.europeana.cloud.tool.validation;

import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.java.utils.ParameterTool;

public class TransformationJobParamValidator implements JobParamValidator {
    @Override
    public void validate(ParameterTool parameterTool) {

        parameterTool.getRequired(JobParamName.DATASET_ID);
        parameterTool.getRequired(JobParamName.EXECUTION_ID);
        parameterTool.getRequired(JobParamName.METIS_DATASET_NAME);
        parameterTool.getRequired(JobParamName.METIS_DATASET_COUNTRY);
        parameterTool.getRequired(JobParamName.METIS_DATASET_LANGUAGE);
        parameterTool.getRequired(JobParamName.METIS_XSLT_URL);
    }
}
