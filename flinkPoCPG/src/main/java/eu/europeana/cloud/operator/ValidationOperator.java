package eu.europeana.cloud.operator;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamValue;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Properties;

public class ValidationOperator extends ProcessFunction<ExecutionRecord, ExecutionRecordResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationOperator.class);
    private static final String EDM_SORTER_FILE_URL = "http://ftp.eanadev.org/schema_zips/edm_sorter_20230809.xsl";
    private ParameterTool parameterTool;
    private transient XsltTransformer transformer;
    private transient ValidationExecutionService validationService;
    private long taskId;
    private String schema;
    private String rootFileLocation;
    private String schematronFileLocation;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOGGER.info("Opening validation operator");
        parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
        taskId = parameterTool.getLong(JobParamName.TASK_ID);
        Properties validationProperties = prepareProperties();
        switch (parameterTool.get(JobParamName.VALIDATION_TYPE)) {
            case JobParamValue.VALIDATION_INTERNAL -> {
                schema = validationProperties.getProperty("predefinedSchemas.edm-internal.url");
                rootFileLocation = validationProperties.getProperty("predefinedSchemas.edm-internal.rootLocation");
                schematronFileLocation = validationProperties.getProperty("predefinedSchemas.edm-internal.schematronLocation");
            }
            case JobParamValue.VALIDATION_EXTERNAL -> {
                schema = validationProperties.getProperty("predefinedSchemas.edm-external.url");
                rootFileLocation = validationProperties.getProperty("predefinedSchemas.edm-external.rootLocation");
                schematronFileLocation = validationProperties.getProperty("predefinedSchemas.edm-external.schematronLocation");
            }
            default -> throw new IllegalStateException("Unexpected value: " + parameterTool.get(JobParamName.VALIDATION_TYPE));
        }
        validationService = new ValidationExecutionService(validationProperties);
        transformer = new XsltTransformer(EDM_SORTER_FILE_URL);
    }

    @Override
    public void processElement(ExecutionRecord sourceRecord, ProcessFunction<ExecutionRecord, ExecutionRecordResult>.Context ctx, Collector<ExecutionRecordResult> out) throws Exception {
        LOGGER.debug("Validating record with id {} on instance: {}", sourceRecord.getExecutionRecordKey().getRecordId(), this);
        ExecutionRecordResult resultRecord = prepareResultRecord(sourceRecord);
        String sortedDocument = reorderFileContent(resultRecord);
        validateFile(resultRecord, sortedDocument);
        out.collect(resultRecord);
    }

    private ExecutionRecordResult prepareResultRecord(ExecutionRecord sourceRecord) {
      return ExecutionRecordResult.from(sourceRecord, taskId, parameterTool.get(JobParamName.VALIDATION_TYPE));
    }



    private String reorderFileContent(ExecutionRecordResult executionRecordResult) throws TransformationException {
        LOGGER.debug("Reordering the file");
        StringWriter writer = transformer.transform(executionRecordResult.getRecordData().getBytes(), null);
        return writer.toString();
    }

    private void validateFile(ExecutionRecordResult executionRecordResult, String sortedDocument) {
        ValidationResult result =
            validationService.singleValidation(
                schema,
                rootFileLocation,
                schematronFileLocation,
                sortedDocument
            );
        if (result.isSuccess()) {
            LOGGER.debug("Validation Success for datasetId {}, recordId {}", executionRecordResult.getExecutionRecord().getExecutionRecordKey().getDatasetId(),
                    executionRecordResult.getRecordId());
        } else {
            LOGGER.info("Validation Failure for datasetId {}, recordId {}", executionRecordResult.getExecutionRecord().getExecutionRecordKey().getDatasetId(),
                    executionRecordResult.getRecordId());
            executionRecordResult.setRecordData("");
            executionRecordResult.setException(result.getMessage());
        }
    }

    private Properties prepareProperties() {
        Properties properties = new Properties();

        properties.setProperty("predefinedSchemas", "localhost");

        properties.setProperty("predefinedSchemas.edm-internal.url",
                "http://ftp.eanadev.org/schema_zips/europeana_schemas-20220809.zip");
        properties.setProperty("predefinedSchemas.edm-internal.rootLocation", "EDM-INTERNAL.xsd");
        properties.setProperty("predefinedSchemas.edm-internal.schematronLocation", "schematron/schematron-internal.xsl");

        properties.setProperty("predefinedSchemas.edm-external.url",
                "http://ftp.eanadev.org/schema_zips/europeana_schemas-20220809.zip");
        properties.setProperty("predefinedSchemas.edm-external.rootLocation", "EDM.xsd");
        properties.setProperty("predefinedSchemas.edm-external.schematronLocation", "schematron/schematron.xsl");

        return properties;

    }
}
