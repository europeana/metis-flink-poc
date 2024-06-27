package eu.europeana.cloud.tool.validation;

import eu.europeana.cloud.tool.JobParamName;
import org.apache.flink.api.java.utils.ParameterTool;

public class OAIJobParamValidator implements JobParamValidator {
    @Override
    public void validate(ParameterTool parameterTool) {

        parameterTool.getRequired(JobParamName.DATASET_ID);
        parameterTool.getRequired(JobParamName.SET_SPEC);
        parameterTool.getRequired(JobParamName.METADATA_PREFIX);
        parameterTool.getRequired(JobParamName.OAI_REPOSITORY_URL);

    }
}
