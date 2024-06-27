package eu.europeana.cloud.tool.validation;

import org.apache.flink.api.java.utils.ParameterTool;

public interface JobParamValidator {

    void validate(ParameterTool parameterTool);
}
